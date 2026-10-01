# abstractmethod enforces that subclasses must implement scrape().
# Python raises TypeError at import time if a subclass omits it —
# catching the mistake early rather than silently at runtime.
from abc import ABC, abstractmethod

# get_logger returns a structlog BoundLogger configured with JSON output,
# ISO timestamps, and log level — defined once in shared, used everywhere.
from shared.logging.logger import get_logger

# Used to stamp each scrape run with a UTC start time in log output.
from datetime import datetime, timezone


class BaseScraper(ABC):
    """Abstract base class for all ingestion scrapers.

    Subclasses must implement scrape(). The ABC machinery enforces this —
    instantiating a subclass that omits scrape() raises TypeError immediately.
    """

    def __init__(self, source_name: str) -> None:
        # Identifies the data source in logs and S3 paths (e.g. "yelp_api", "reddit").
        # Passed into S3Writer to build the correct partition prefix.
        self.source_name = source_name

        # Namespace the logger to this module ("ingestion.base") so log output
        # can be filtered or routed by source in CloudWatch or any log aggregator.
        self.logger = get_logger(__name__)

    @abstractmethod
    def scrape(self) -> list[dict]:
        """Fetch raw records from the data source.

        Must be implemented by every subclass. Return one dict per record
        (review, post, article etc.) — raw data only, no transformation.
        S3Writer handles serialisation; the ETL layer handles normalisation.
        """
        pass

    def run(self) -> list[dict]:
        """Public entrypoint called by Airflow operators and Makefile targets.

        Wraps scrape() with consistent logging so every subclass gets
        observability without duplicating log calls.
        """
        # Structlog keyword args become structured JSON fields —
        # searchable and filterable in any downstream log system.
        self.logger.info(
            "scrape started",
            source=self.source_name,
            started_at=datetime.now(timezone.utc).isoformat(),
        )

        records = self.scrape()

        # Record count logged at completion — a sudden drop to 0 on an
        # active source is a useful signal for an Airflow alert later.
        self.logger.info(
            "scrape complete",
            source=self.source_name,
            record_count=len(records),
        )

        return records