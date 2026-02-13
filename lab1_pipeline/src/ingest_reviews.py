import json
import logging
from google_play_scraper import reviews, Sort
from src import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pagination settings
REVIEWS_PER_PAGE = 200  # Smaller batches to avoid rate limiting
MAX_PAGES = 10          # Maximum number of pages to fetch (200 * 10 = 2000 reviews max)


def run(output_file=None):
    """
    Fetches raw reviews using pagination and saves them as JSONL (one JSON object per line).
    Uses an append strategy to prevent data loss if the script crashes mid-way.
    """
    if output_file is None:
        output_file = config.RAW_DIR / config.REVIEWS_FILENAME

    app_id = config.TARGET_APP_ID
    logger.info(f"Fetching reviews for app: {app_id}")

    config.RAW_DIR.mkdir(parents=True, exist_ok=True)

    # Clear the file at the start of a fresh run
    with open(output_file, "w", encoding="utf-8") as f:
        pass  # Truncate the file

    continuation_token = None
    total_fetched = 0

    try:
        for page in range(MAX_PAGES):
            logger.info(f"Fetching page {page + 1}/{MAX_PAGES}...")

            result, continuation_token = reviews(
                app_id,
                lang='en',
                country='us',
                sort=Sort.NEWEST,
                count=REVIEWS_PER_PAGE,
                continuation_token=continuation_token
            )

            if not result:
                logger.info("No more reviews to fetch.")
                break

            # Append each review as a JSON line (JSONL format)
            # This "write with append in the loop" strategy prevents data loss
            with open(output_file, "a", encoding="utf-8") as f:
                for review in result:
                    f.write(json.dumps(review, default=str) + "\n")

            total_fetched += len(result)
            logger.info(f"Appended {len(result)} reviews (total: {total_fetched})")

            # Stop if no more pages
            if continuation_token is None:
                logger.info("Reached end of reviews.")
                break

        logger.info(f"Completed! Saved {total_fetched} reviews to {output_file}")

    except Exception as e:
        logger.error(f"Failed to ingest reviews at page {page + 1}: {e}")
        logger.info(f"Partial data saved: {total_fetched} reviews written before failure.")


if __name__ == "__main__":
    run()
