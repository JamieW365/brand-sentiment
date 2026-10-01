import requests
import time
from shared.config.settings import get_settings
from shared.logging.logger import get_logger
from ingestion.base import BaseScraper


class YelpAPIScraper(BaseScraper):
    """Scrapes live business reviews from the Yelp Fusion API.

    Hits two endpoints per brand term:
        1. /v3/businesses/search  — find matching businesses
        2. /v3/businesses/{id}/reviews — fetch reviews for each

    Note: Yelp Fusion returns max 3 reviews per business on the free tier.
    Bulk historical reviews are handled separately via the Yelp Open Dataset.
    """

    BASE_URL = "https://api.yelp.com/v3"

    def __init__(self, targets: list[str], location: str = "United Kingdom") -> None:
        """
        Args:
            targets: Brand search terms to scrape (e.g. ["McDonald's", "Greggs"]).
                     Passed at instantiation so the scraper is reusable across
                     different brand sets without subclassing.
            location: Scopes Yelp searches to a geography. Defaults to UK.
        """
        super().__init__(source_name="yelp_api")
        self.targets = targets
        self.location = location

        # Session reuses the TCP connection across all requests to api.yelp.com
        # and lets the auth header be set once rather than on every call.
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {get_settings().yelp_api_key}"})

        self.logger = get_logger(__name__)

    def _search_businesses(self, term: str, limit: int = 50) -> list[dict]:
        """Search for businesses matching a brand term.

        Args:
            term: Brand name to search for.
            limit: Max results to return. 50 is the Yelp API maximum per request.

        Returns:
            List of business dicts, each containing an 'id' used to fetch reviews.
        """
        response = self.session.get(
            url=f"{self.BASE_URL}/businesses/search",
            params={
                "term": term,
                "location": self.location,
                "limit": limit,
            },
            timeout=10,
        )

        response.raise_for_status()

        businesses = response.json().get("businesses", [])

        self.logger.info(
            "businesses found",
            term=term,
            count=len(businesses),
        )

        return businesses

    def _get_reviews(self, business_id: str, business_name: str) -> list[dict]:
        """Fetch reviews for a single business.

        Each review is enriched with business metadata before returning so the
        downstream Glue ETL job can write directly to Postgres without a separate
        business lookup.

        Args:
            business_id: Yelp business ID from the search response.
            business_name: Human-readable name, stored on each review record.

        Returns:
            List of review dicts enriched with business context. Max 3 on the free tier.
        """
        response = self.session.get(
            url=f"{self.BASE_URL}/businesses/{business_id}/reviews",
            timeout=10,
        )

        response.raise_for_status()

        reviews = response.json().get("reviews", [])

        for review in reviews:
            review["business_id"] = business_id
            review["business_name"] = business_name
            review["source"] = "yelp_api"  # consistent source tag across all scrapers

        return reviews

    def scrape(self) -> list[dict]:
        """Fetch reviews for all target brands and return them as a flat list.

        Errors on a single brand or business are logged and skipped rather than
        aborting the run — a failure on one target shouldn't drop data for all others.

        Returns:
            All collected review dicts across every target brand.
        """
        all_reviews = []

        for term in self.targets:
            self.logger.info("scraping brand", term=term)

            try:
                businesses = self._search_businesses(term)
            except requests.RequestException as e:
                self.logger.error("business search failed", term=term, error=str(e))
                continue

            for business in businesses:
                try:
                    reviews = self._get_reviews(
                        business_id=business["id"],
                        business_name=business["name"],
                    )
                    all_reviews.extend(reviews)

                except requests.RequestException as e:
                    self.logger.error(
                        "review fetch failed",
                        business_id=business["id"],
                        error=str(e),
                    )

                # Respect Yelp's rate limits — 250ms between review calls
                time.sleep(0.25)

        return all_reviews


if __name__ == "__main__":
    from shared.logging.logger import setup_logging
    from ingestion.s3_writer import S3Writer

    # setup_logging() must be called once before any logger is used.
    # When running via Airflow this is called in the DAG file instead.
    setup_logging()
    logger = get_logger(__name__)
    
    # Hardcoded for local runs — will be driven by a Postgres brand lookup in production.
    TARGETS = ["McDonald's", "Greggs", "Pret a Manger"]

    scraper = YelpAPIScraper(targets=TARGETS)
    records = scraper.run()

    writer = S3Writer(source_name="yelp_api")
    s3_key = writer.write(records)

    logger.info("ingestion complete", record_count=len(records), s3_key=s3_key)