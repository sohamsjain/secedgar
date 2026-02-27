"""SEC EDGAR API client for fetching 10-K filings and XBRL data."""

import asyncio
import logging
import re

import httpx

from config import config

logger = logging.getLogger(__name__)

# SEC EDGAR base URLs
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
ARCHIVES_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession_nodash}/"
XBRL_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"


class EDGARClient:
    """Async client for SEC EDGAR API with rate limiting and retry logic."""

    def __init__(self):
        self.headers = {
            "User-Agent": config.sec_user_agent,
            "Accept-Encoding": "gzip, deflate",
        }
        self._rate_limit_delay = config.sec_rate_limit_delay
        self._last_request_time = 0.0

    async def _rate_limited_get(
        self, client: httpx.AsyncClient, url: str, max_retries: int = 3
    ) -> httpx.Response:
        """Make a rate-limited GET request with exponential backoff on errors."""
        for attempt in range(max_retries + 1):
            # Respect SEC rate limit
            now = asyncio.get_event_loop().time()
            elapsed = now - self._last_request_time
            if elapsed < self._rate_limit_delay:
                await asyncio.sleep(self._rate_limit_delay - elapsed)

            try:
                self._last_request_time = asyncio.get_event_loop().time()
                response = await client.get(url, headers=self.headers, timeout=30.0)

                if response.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning(
                        "Rate limited by SEC EDGAR, waiting %ds (attempt %d/%d)",
                        wait, attempt + 1, max_retries + 1,
                    )
                    await asyncio.sleep(wait)
                    continue

                response.raise_for_status()
                return response

            except httpx.HTTPStatusError:
                if attempt < max_retries:
                    wait = 2 ** (attempt + 1)
                    logger.warning(
                        "HTTP error fetching %s, retrying in %ds (attempt %d/%d)",
                        url, wait, attempt + 1, max_retries + 1,
                    )
                    await asyncio.sleep(wait)
                else:
                    raise
            except httpx.RequestError:
                if attempt < max_retries:
                    wait = 2 ** (attempt + 1)
                    logger.warning(
                        "Request error fetching %s, retrying in %ds (attempt %d/%d)",
                        url, wait, attempt + 1, max_retries + 1,
                    )
                    await asyncio.sleep(wait)
                else:
                    raise

        raise RuntimeError(f"Failed to fetch {url} after {max_retries + 1} attempts")

    async def get_latest_10k_accession(self, cik: str) -> str | None:
        """Fetch the most recent 10-K accession number for a given CIK.

        Returns the accession number string or None if no 10-K found.
        """
        padded_cik = cik.zfill(10)
        url = SUBMISSIONS_URL.format(cik=padded_cik)
        logger.info("Fetching submissions for CIK %s", cik)

        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await self._rate_limited_get(client, url)
            data = response.json()

            recent = data.get("filings", {}).get("recent", {})
            forms = recent.get("form", [])
            accessions = recent.get("accessionNumber", [])

            for form, accession in zip(forms, accessions):
                if form == "10-K":
                    logger.info("Found 10-K accession: %s", accession)
                    return accession

            logger.warning("No 10-K filing found for CIK %s", cik)
            return None

    async def get_filing_document_url(self, cik: str, accession: str) -> str | None:
        """Find the primary .htm document URL for a filing.

        Returns the full URL to the primary document or None if not found.
        """
        accession_nodash = accession.replace("-", "")
        index_url = ARCHIVES_URL.format(cik=cik, accession_nodash=accession_nodash)
        index_json_url = (
            f"https://www.sec.gov/Archives/edgar/data/{cik}/"
            f"{accession_nodash}/{accession}-index.json"
        )

        async with httpx.AsyncClient(follow_redirects=True) as client:
            try:
                response = await self._rate_limited_get(client, index_json_url)
                data = response.json()
                items = data.get("directory", {}).get("item", [])

                # Look for primary 10-K document (usually the largest .htm file)
                htm_files = [
                    item for item in items
                    if item.get("name", "").lower().endswith((".htm", ".html"))
                    and "index" not in item.get("name", "").lower()
                    and "R" not in item.get("name", "").split(".")[0]  # skip XBRL viewer
                ]

                if htm_files:
                    # Sort by size descending - primary doc is typically the largest
                    htm_files.sort(
                        key=lambda x: int(x.get("size", "0")), reverse=True
                    )
                    primary_doc = htm_files[0]["name"]
                    full_url = f"{index_url}{primary_doc}"
                    logger.info("Found primary document: %s", full_url)
                    return full_url

            except Exception:
                logger.debug("JSON index not available, falling back to HTML index")

            # Fallback: parse the HTML index page
            try:
                response = await self._rate_limited_get(client, index_url)
                html = response.text
                # Find .htm links that are likely the primary document
                pattern = r'href="([^"]+\.htm[l]?)"'
                matches = re.findall(pattern, html, re.IGNORECASE)
                for match in matches:
                    if "index" not in match.lower():
                        full_url = f"https://www.sec.gov{match}" if match.startswith("/") else f"{index_url}{match}"
                        logger.info("Found primary document (fallback): %s", full_url)
                        return full_url
            except Exception as e:
                logger.error("Failed to find primary document: %s", e)

        return None

    async def fetch_filing_html(self, url: str) -> str:
        """Stream the raw HTML of a filing with chunked transfer.

        Handles rate limiting and respects SEC's 10 req/sec limit.
        """
        logger.info("Fetching filing HTML from %s", url)
        chunks = []

        async with httpx.AsyncClient(follow_redirects=True) as client:
            # Respect rate limit before starting the stream
            now = asyncio.get_event_loop().time()
            elapsed = now - self._last_request_time
            if elapsed < self._rate_limit_delay:
                await asyncio.sleep(self._rate_limit_delay - elapsed)

            self._last_request_time = asyncio.get_event_loop().time()

            async with client.stream(
                "GET", url, headers=self.headers, timeout=60.0
            ) as response:
                response.raise_for_status()
                async for chunk in response.aiter_text(chunk_size=8192):
                    chunks.append(chunk)

        html = "".join(chunks)
        logger.info("Fetched %d characters of HTML", len(html))
        return html

    async def get_xbrl_facts(self, cik: str) -> dict:
        """Fetch XBRL company facts and extract key financial metrics.

        Returns dict with revenue, net_income, total_assets for last 3 years.
        """
        padded_cik = cik.zfill(10)
        url = XBRL_FACTS_URL.format(cik=padded_cik)
        logger.info("Fetching XBRL facts for CIK %s", cik)

        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await self._rate_limited_get(client, url)
            data = response.json()

        facts = data.get("facts", {})
        us_gaap = facts.get("us-gaap", {})

        result = {
            "company_name": data.get("entityName", "Unknown"),
            "cik": cik,
            "revenue": _extract_annual_values(us_gaap, [
                "Revenues", "RevenueFromContractWithCustomerExcludingAssessedTax",
                "SalesRevenueNet", "RevenueFromContractWithCustomerIncludingAssessedTax",
            ]),
            "net_income": _extract_annual_values(us_gaap, [
                "NetIncomeLoss", "ProfitLoss",
            ]),
            "total_assets": _extract_annual_values(us_gaap, [
                "Assets",
            ]),
        }

        logger.info(
            "Extracted XBRL facts: %d revenue entries, %d income entries, %d asset entries",
            len(result["revenue"]), len(result["net_income"]), len(result["total_assets"]),
        )
        return result


def _extract_annual_values(us_gaap: dict, concept_names: list[str]) -> list[dict]:
    """Extract the last 3 years of annual (10-K) values for given XBRL concepts."""
    for concept in concept_names:
        concept_data = us_gaap.get(concept, {})
        units = concept_data.get("units", {})
        usd_values = units.get("USD", [])

        # Filter for 10-K (annual) filings only
        annual = [
            v for v in usd_values
            if v.get("form") == "10-K" and v.get("fp") == "FY"
        ]

        if annual:
            # Sort by end date descending and take last 3
            annual.sort(key=lambda x: x.get("end", ""), reverse=True)
            # Deduplicate by fiscal year end date
            seen_dates = set()
            unique = []
            for entry in annual:
                end_date = entry.get("end", "")
                if end_date not in seen_dates:
                    seen_dates.add(end_date)
                    unique.append({
                        "period_end": end_date,
                        "value": entry.get("val"),
                        "fiscal_year": entry.get("fy"),
                    })
                if len(unique) >= 3:
                    break
            return unique

    return []
