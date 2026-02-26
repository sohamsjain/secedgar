"""HTML parsing and section extraction for SEC 10-K filings."""

import logging
import re

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Section patterns: match "Item 1.", "ITEM 1.", "Item 1 ", etc.
# We use the LAST match to skip the table of contents.
SECTION_PATTERNS = {
    "business": re.compile(
        r"(?:^|\n)\s*(?:ITEM|Item)\s+1[\.\s:]+\s*(?:BUSINESS|Business)",
        re.IGNORECASE,
    ),
    "risk_factors": re.compile(
        r"(?:^|\n)\s*(?:ITEM|Item)\s+1A[\.\s:]+\s*(?:RISK\s+FACTORS|Risk\s+Factors)",
        re.IGNORECASE,
    ),
    "mda": re.compile(
        r"(?:^|\n)\s*(?:ITEM|Item)\s+7[\.\s:]+\s*(?:MANAGEMENT|Management)",
        re.IGNORECASE,
    ),
    "market_risk": re.compile(
        r"(?:^|\n)\s*(?:ITEM|Item)\s+7A[\.\s:]+\s*(?:QUANTITATIVE|Quantitative)",
        re.IGNORECASE,
    ),
    "financials": re.compile(
        r"(?:^|\n)\s*(?:ITEM|Item)\s+8[\.\s:]+\s*(?:FINANCIAL\s+STATEMENTS|Financial\s+Statements)",
        re.IGNORECASE,
    ),
}

# Ordering of sections as they appear in a 10-K
SECTION_ORDER = ["business", "risk_factors", "mda", "market_risk", "financials"]

# Minimum word count thresholds for quality validation
MIN_WORD_COUNTS = {
    "business": 500,
    "risk_factors": 300,
    "mda": 500,
    "market_risk": 100,
    "financials": 100,
}


class FilingParser:
    """Parses 10-K filing HTML and extracts structured sections."""

    def clean_html(self, html: str) -> str:
        """Remove markup and extract clean text from filing HTML.

        Strips script, style, ix:header, ix:hidden tags and returns
        plain text with newline separators.
        """
        soup = BeautifulSoup(html, "lxml")

        # Remove non-content tags
        for tag_name in ["script", "style", "ix:header", "ix:hidden"]:
            for tag in soup.find_all(tag_name):
                tag.decompose()

        # Also remove hidden divs commonly used in EDGAR filings
        for tag in soup.find_all(style=re.compile(r"display\s*:\s*none", re.IGNORECASE)):
            tag.decompose()

        text = soup.get_text(separator="\n")

        # Clean up excessive whitespace while preserving paragraph structure
        lines = []
        for line in text.splitlines():
            stripped = line.strip()
            if stripped:
                lines.append(stripped)

        return "\n".join(lines)

    def extract_sections(self, text: str) -> dict[str, str]:
        """Extract key sections from cleaned 10-K text using regex matching.

        Uses the LAST regex match for each section header to skip
        the table of contents. Slices text between consecutive
        section positions.
        """
        # Find the last occurrence of each section header
        section_positions = {}
        for name, pattern in SECTION_PATTERNS.items():
            matches = list(pattern.finditer(text))
            if matches:
                # Take the LAST match to skip table of contents
                section_positions[name] = matches[-1].start()
                logger.debug(
                    "Found section '%s' at position %d (match %d of %d)",
                    name, matches[-1].start(), len(matches), len(matches),
                )
            else:
                logger.warning("Section '%s' not found in filing text", name)

        if not section_positions:
            logger.error("No sections found in filing text")
            return {}

        # Sort found sections by their position in the document
        sorted_sections = sorted(section_positions.items(), key=lambda x: x[1])

        # Extract text between consecutive section positions
        sections = {}
        for i, (name, start_pos) in enumerate(sorted_sections):
            if i + 1 < len(sorted_sections):
                end_pos = sorted_sections[i + 1][1]
            else:
                # Last section: take up to 50k chars or end of document
                end_pos = min(start_pos + 50000, len(text))

            section_text = text[start_pos:end_pos].strip()
            sections[name] = section_text
            word_count = len(section_text.split())
            logger.info("Extracted section '%s': %d words", name, word_count)

        return sections

    def validate_sections(self, sections: dict[str, str]) -> dict:
        """Validate extracted sections against minimum length thresholds.

        Returns the sections dict with an added 'quality_score' key (0-100).
        """
        total_checks = len(MIN_WORD_COUNTS)
        passed_checks = 0

        for name, min_words in MIN_WORD_COUNTS.items():
            if name in sections:
                word_count = len(sections[name].split())
                if word_count >= min_words:
                    passed_checks += 1
                else:
                    logger.warning(
                        "Section '%s' is too short: %d words (minimum %d)",
                        name, word_count, min_words,
                    )
            else:
                logger.warning("Section '%s' is missing from extraction", name)

        quality_score = int((passed_checks / total_checks) * 100) if total_checks > 0 else 0

        result = dict(sections)
        result["quality_score"] = quality_score
        logger.info("Section quality score: %d/100 (%d/%d checks passed)",
                     quality_score, passed_checks, total_checks)
        return result

    def extract_metadata(self, html: str) -> dict:
        """Extract filing metadata from the HTML header.

        Pulls company name, period of report, fiscal year end, and CIK.
        """
        metadata = {
            "company_name": "",
            "period_of_report": "",
            "fiscal_year_end": "",
            "cik": "",
        }

        # Try structured SGML header patterns first
        patterns = {
            "company_name": [
                re.compile(r"COMPANY\s+CONFORMED\s+NAME:\s*(.+)", re.IGNORECASE),
                re.compile(r"<COMPANY-NAME>(.+?)</COMPANY-NAME>", re.IGNORECASE),
            ],
            "period_of_report": [
                re.compile(r"CONFORMED\s+PERIOD\s+OF\s+REPORT:\s*(\d{8})", re.IGNORECASE),
                re.compile(r"PERIOD\s+OF\s+REPORT:\s*(\d{4}-\d{2}-\d{2})", re.IGNORECASE),
            ],
            "fiscal_year_end": [
                re.compile(r"FISCAL\s+YEAR\s+END:\s*(\d{4})", re.IGNORECASE),
            ],
            "cik": [
                re.compile(r"CENTRAL\s+INDEX\s+KEY:\s*(\d+)", re.IGNORECASE),
                re.compile(r"CIK[=:]\s*(\d+)", re.IGNORECASE),
            ],
        }

        # Search only the first portion of HTML for header data
        header_region = html[:5000]

        for field, field_patterns in patterns.items():
            for pattern in field_patterns:
                match = pattern.search(header_region)
                if match:
                    metadata[field] = match.group(1).strip()
                    break

        # Fallback: try to extract company name from <title> tag
        if not metadata["company_name"]:
            soup = BeautifulSoup(html[:10000], "lxml")
            title = soup.find("title")
            if title and title.string:
                metadata["company_name"] = title.string.strip()

        logger.info("Extracted metadata: %s", metadata)
        return metadata
