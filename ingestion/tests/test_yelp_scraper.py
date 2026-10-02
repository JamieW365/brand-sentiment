import pytest
from unittest.mock import MagicMock, patch
import requests
from ingestion.yelp_api.scraper import YelpAPIScraper


@pytest.fixture
def scraper():
    """Provide a YelpAPIScraper instance with settings patched out.

    Patches get_settings() at the point it is imported in the scraper module
    so instantiation does not attempt to read a .env file that won't exist
    in the test environment.
    """
    with patch("ingestion.yelp_api.scraper.get_settings") as mock_settings:
        mock_settings.return_value.yelp_api_key = "test-api-key"
        yield YelpAPIScraper(targets=["McDonald's"])


class TestSearchBusinesses:

    def test_returns_businesses_on_success(self, scraper):
        """Returns the list of business dicts from a successful Yelp search response."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "businesses": [{"id": "biz-1", "name": "McDonald's"}]
        }
        scraper.session.get = MagicMock(return_value=mock_response)

        result = scraper._search_businesses("McDonald's")

        assert len(result) == 1
        assert result[0]["id"] == "biz-1"

    def test_returns_empty_list_when_no_businesses(self, scraper):
        """Returns an empty list when Yelp finds no matching businesses."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"businesses": []}
        scraper.session.get = MagicMock(return_value=mock_response)

        result = scraper._search_businesses("Unknown Brand")

        assert result == []

    def test_raises_on_http_error(self, scraper):
        """Propagates HTTPError so scrape() can catch, log, and skip the term.

        raise_for_status() raises on any 4xx/5xx response — the method must
        not swallow it.
        """
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("429")
        scraper.session.get = MagicMock(return_value=mock_response)

        with pytest.raises(requests.HTTPError):
            scraper._search_businesses("McDonald's")

    def test_search_calls_correct_url_and_params(self, scraper):
        """session.get should be called with the businesses/search endpoint and correct params."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"businesses": []}
        scraper.session.get = MagicMock(return_value=mock_response)

        scraper._search_businesses("McDonald's", limit=50)

        scraper.session.get.assert_called_once_with(
            url="https://api.yelp.com/v3/businesses/search",
            params={"term": "McDonald's", "location": "United Kingdom", "limit": 50},
            timeout=10,
        )


class TestGetReviews:

    def test_enriches_reviews_with_business_context(self, scraper):
        """Enriches every review dict with business_id, business_name, and source.

        These fields are added so the downstream Glue ETL job can write directly
        to Postgres without a separate business lookup.
        """
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "reviews": [{"id": "rev-1", "text": "Great food!"}]
        }
        scraper.session.get = MagicMock(return_value=mock_response)

        reviews = scraper._get_reviews("biz-1", "McDonald's")

        assert reviews[0]["business_id"] == "biz-1"
        assert reviews[0]["business_name"] == "McDonald's"
        assert reviews[0]["source"] == "yelp_api"

    def test_returns_empty_list_when_no_reviews(self, scraper):
        """Returns an empty list when a business has no reviews."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"reviews": []}
        scraper.session.get = MagicMock(return_value=mock_response)

        reviews = scraper._get_reviews("biz-1", "McDonald's")

        assert reviews == []

    def test_raises_on_http_error(self, scraper):
        """Propagates HTTPError so scrape() can catch, log, and skip the business."""
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404")
        scraper.session.get = MagicMock(return_value=mock_response)

        with pytest.raises(requests.HTTPError):
            scraper._get_reviews("biz-1", "McDonald's")

    def test_reviews_calls_correct_url(self, scraper):
        """session.get should be called with the correct business-specific reviews endpoint."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"reviews": []}
        scraper.session.get = MagicMock(return_value=mock_response)

        scraper._get_reviews("biz-1", "McDonald's")

        scraper.session.get.assert_called_once_with(
            url="https://api.yelp.com/v3/businesses/biz-1/reviews",
            timeout=10,
        )


class TestScrape:

    @patch("ingestion.yelp_api.scraper.time.sleep")
    def test_skips_term_on_search_failure(self, _mock_sleep, scraper):
        """Skips a brand term and returns an empty list when business search fails.

        A single failed term must not abort ingestion for remaining targets.
        """
        scraper._search_businesses = MagicMock(
            side_effect=requests.RequestException("timeout")
        )

        result = scraper.scrape()

        assert result == []

    @patch("ingestion.yelp_api.scraper.time.sleep")
    def test_skips_business_on_review_failure(self, _mock_sleep, scraper):
        """Skips a business and continues when its review fetch fails."""
        scraper._search_businesses = MagicMock(
            return_value=[{"id": "biz-1", "name": "McDonald's"}]
        )
        scraper._get_reviews = MagicMock(
            side_effect=requests.RequestException("timeout")
        )

        result = scraper.scrape()

        assert result == []

    @patch("ingestion.yelp_api.scraper.time.sleep")
    def test_sleep_called_once_per_business(self, mock_sleep, scraper):
        """Calls sleep once per business to respect Yelp's rate limits."""
        scraper._search_businesses = MagicMock(
            return_value=[
                {"id": "biz-1", "name": "McDonald's"},
                {"id": "biz-2", "name": "McDonald's City"},
            ]
        )
        scraper._get_reviews = MagicMock(return_value=[])

        scraper.scrape()

        # Two businesses — sleep should be called twice at 0.25s each
        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(0.25)

    @patch("ingestion.yelp_api.scraper.time.sleep")
    def test_returns_flat_list_across_all_businesses(self, _mock_sleep, scraper):
        """Flattens reviews from multiple businesses into a single list."""
        scraper._search_businesses = MagicMock(
            return_value=[
                {"id": "biz-1", "name": "McDonald's"},
                {"id": "biz-2", "name": "McDonald's City"},
            ]
        )
        scraper._get_reviews = MagicMock(
            return_value=[{"id": "rev-1", "text": "Good"}]
        )

        result = scraper.scrape()

        # One review per business × two businesses = two total
        assert len(result) == 2

    @patch("ingestion.yelp_api.scraper.time.sleep")
    def test_sleep_called_even_on_review_failure(self, mock_sleep, scraper):
        """sleep() should fire even when _get_reviews raises — it sits outside the try/except."""
        scraper._search_businesses = MagicMock(
            return_value=[{"id": "biz-1", "name": "McDonald's"}]
        )
        scraper._get_reviews = MagicMock(
            side_effect=requests.RequestException("timeout")
        )

        scraper.scrape()

        # One business attempted, one sleep expected regardless of failure
        assert mock_sleep.call_count == 1
        mock_sleep.assert_called_with(0.25)
