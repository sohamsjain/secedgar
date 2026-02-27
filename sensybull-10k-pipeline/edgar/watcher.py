"""RSS feed polling for new SEC EDGAR 10-K filings."""

import asyncio
import logging
import re

import feedparser
import httpx

from config import config

logger = logging.getLogger(__name__)

EDGAR_RSS_URL = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
    "?action=getcurrent&type=10-K&dateb=&owner=include&count=40&output=atom"
)


class FilingWatcher:
    """Polls the EDGAR RSS feed for new 10-K filings in the watchlist."""

    def __init__(self, queue: asyncio.Queue, db=None):
        self.queue = queue
        self.db = db
        self.seen_accessions: set[str] = set()
        self._running = False
        # Build reverse lookup: CIK -> ticker
        self._cik_to_ticker = {cik: ticker for ticker, cik in config.watchlist.items()}

    async def _load_seen_from_db(self):
        """Load previously processed accessions from the database."""
        if self.db:
            try:
                rows = self.db.get_all_accessions()
                self.seen_accessions.update(rows)
                logger.info("Loaded %d seen accessions from DB", len(rows))
            except Exception as e:
                logger.warning("Could not load seen accessions from DB: %s", e)

    async def poll_once(self) -> list[dict]:
        """Poll the RSS feed once and return new filing events."""
        new_filings = []

        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(
                EDGAR_RSS_URL,
                headers={"User-Agent": config.sec_user_agent},
                timeout=30.0,
            )
            response.raise_for_status()

        feed = feedparser.parse(response.text)

        for entry in feed.entries:
            title = entry.get("title", "")
            link = entry.get("link", "")
            summary = entry.get("summary", "")

            # Extract accession number from the link or summary
            accession_match = re.search(r"(\d{10}-\d{2}-\d{6})", link + " " + summary)
            if not accession_match:
                continue
            accession = accession_match.group(1)

            if accession in self.seen_accessions:
                continue

            # Extract CIK from the link
            cik_match = re.search(r"/data/(\d+)/", link)
            if not cik_match:
                # Try from the title or summary
                cik_match = re.search(r"CIK[=:]?\s*(\d+)", title + " " + summary)
            if not cik_match:
                continue
            cik = cik_match.group(1)

            # Check if this CIK is in our watchlist
            ticker = self._cik_to_ticker.get(cik)
            if not ticker:
                # Try with zero-padded CIK
                for wl_cik, wl_ticker in self._cik_to_ticker.items():
                    if cik.lstrip("0") == wl_cik.lstrip("0"):
                        ticker = wl_ticker
                        break

            if not ticker:
                continue

            self.seen_accessions.add(accession)
            filing_event = {
                "ticker": ticker,
                "cik": config.watchlist[ticker],
                "accession": accession,
                "title": title,
            }
            new_filings.append(filing_event)
            logger.info("New 10-K filing detected: %s (%s)", ticker, accession)

        return new_filings

    async def run(self):
        """Continuously poll for new filings at POLL_INTERVAL."""
        self._running = True
        await self._load_seen_from_db()
        logger.info(
            "Starting filing watcher (poll interval: %ds, watchlist: %s)",
            config.poll_interval,
            list(config.watchlist.keys()),
        )

        while self._running:
            try:
                new_filings = await self.poll_once()
                for filing in new_filings:
                    await self.queue.put(filing)
                    logger.info("Queued filing event: %s", filing["ticker"])

                if not new_filings:
                    logger.debug("No new filings found in this poll cycle")

            except Exception as e:
                logger.error("Error polling EDGAR RSS feed: %s", e)

            await asyncio.sleep(config.poll_interval)

    def stop(self):
        """Signal the watcher to stop after the current poll cycle."""
        self._running = False
        logger.info("Filing watcher stopping")
