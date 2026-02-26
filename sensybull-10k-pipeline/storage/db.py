"""SQLite storage with a Supabase-compatible interface for the 10-K pipeline."""

import json
import logging
import sqlite3
from datetime import datetime, timezone

from config import config

logger = logging.getLogger(__name__)


class SQLiteStorage:
    """Local SQLite storage with an interface designed for easy Supabase migration."""

    def __init__(self, db_path: str | None = None):
        self.db_path = db_path or config.db_path
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._create_tables()
        logger.info("Database initialized at %s", self.db_path)

    def _create_tables(self):
        """Create all required tables if they don't exist."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS filings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                cik TEXT NOT NULL,
                accession TEXT NOT NULL UNIQUE,
                filed_date TEXT,
                fiscal_year TEXT,
                processed_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS filing_sections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filing_id INTEGER NOT NULL,
                section_name TEXT NOT NULL,
                raw_text TEXT NOT NULL,
                word_count INTEGER NOT NULL,
                quality_score INTEGER,
                FOREIGN KEY (filing_id) REFERENCES filings(id)
            );

            CREATE TABLE IF NOT EXISTS analyses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filing_id INTEGER NOT NULL,
                section_name TEXT NOT NULL,
                model_used TEXT NOT NULL,
                result_json TEXT NOT NULL,
                tokens_used INTEGER,
                created_at TEXT NOT NULL,
                FOREIGN KEY (filing_id) REFERENCES filings(id)
            );

            CREATE TABLE IF NOT EXISTS investment_briefs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filing_id INTEGER NOT NULL,
                ticker TEXT NOT NULL,
                brief_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (filing_id) REFERENCES filings(id)
            );

            CREATE INDEX IF NOT EXISTS idx_filings_ticker ON filings(ticker);
            CREATE INDEX IF NOT EXISTS idx_filings_accession ON filings(accession);
            CREATE INDEX IF NOT EXISTS idx_briefs_ticker ON investment_briefs(ticker);
        """)
        self.conn.commit()

    def save_filing(
        self,
        ticker: str,
        cik: str,
        accession: str,
        filed_date: str = "",
        fiscal_year: str = "",
    ) -> int:
        """Save a filing record and return its ID."""
        now = datetime.now(timezone.utc).isoformat()
        cursor = self.conn.execute(
            """INSERT INTO filings (ticker, cik, accession, filed_date, fiscal_year, processed_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (ticker, cik, accession, filed_date, fiscal_year, now),
        )
        self.conn.commit()
        filing_id = cursor.lastrowid
        logger.info("Saved filing %s (id=%d) for %s", accession, filing_id, ticker)
        return filing_id

    def save_section(
        self,
        filing_id: int,
        section_name: str,
        raw_text: str,
        word_count: int,
        quality_score: int | None = None,
    ) -> int:
        """Save an extracted section and return its ID."""
        cursor = self.conn.execute(
            """INSERT INTO filing_sections (filing_id, section_name, raw_text, word_count, quality_score)
               VALUES (?, ?, ?, ?, ?)""",
            (filing_id, section_name, raw_text, word_count, quality_score),
        )
        self.conn.commit()
        return cursor.lastrowid

    def save_analysis(
        self,
        filing_id: int,
        section_name: str,
        model_used: str,
        result_json: dict,
        tokens_used: int | None = None,
    ) -> int:
        """Save an analysis result and return its ID."""
        now = datetime.now(timezone.utc).isoformat()
        cursor = self.conn.execute(
            """INSERT INTO analyses (filing_id, section_name, model_used, result_json, tokens_used, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (filing_id, section_name, model_used, json.dumps(result_json), tokens_used, now),
        )
        self.conn.commit()
        return cursor.lastrowid

    def save_brief(self, filing_id: int, ticker: str, brief_json: dict) -> int:
        """Save an investment brief and return its ID."""
        now = datetime.now(timezone.utc).isoformat()
        cursor = self.conn.execute(
            """INSERT INTO investment_briefs (filing_id, ticker, brief_json, created_at)
               VALUES (?, ?, ?, ?)""",
            (filing_id, ticker, json.dumps(brief_json), now),
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_latest_filing(self, ticker: str) -> dict | None:
        """Get the most recent filing for a ticker."""
        row = self.conn.execute(
            """SELECT * FROM filings WHERE ticker = ? ORDER BY processed_at DESC LIMIT 1""",
            (ticker,),
        ).fetchone()
        return dict(row) if row else None

    def get_brief(self, ticker: str) -> dict | None:
        """Get the most recent investment brief for a ticker."""
        row = self.conn.execute(
            """SELECT * FROM investment_briefs WHERE ticker = ?
               ORDER BY created_at DESC LIMIT 1""",
            (ticker,),
        ).fetchone()
        if row:
            result = dict(row)
            result["brief_json"] = json.loads(result["brief_json"])
            return result
        return None

    def get_all_accessions(self) -> set[str]:
        """Get all known accession numbers (for deduplication in the watcher)."""
        rows = self.conn.execute("SELECT accession FROM filings").fetchall()
        return {row["accession"] for row in rows}

    def filing_exists(self, accession: str) -> bool:
        """Check if a filing with this accession has already been processed."""
        row = self.conn.execute(
            "SELECT 1 FROM filings WHERE accession = ?", (accession,)
        ).fetchone()
        return row is not None

    def export_brief_markdown(self, ticker: str) -> str:
        """Format the latest investment brief as clean readable markdown."""
        brief_row = self.get_brief(ticker)
        if not brief_row:
            return f"No investment brief found for {ticker}."

        b = brief_row["brief_json"]

        signal = b.get("overall_signal", "unknown")
        signal_emoji = {"positive": "+", "neutral": "~", "negative": "-"}.get(signal, "?")

        lines = [
            f"# Investment Brief: {b.get('company', ticker)} ({b.get('ticker', ticker)})",
            f"**Fiscal Year:** {b.get('fiscal_year', 'N/A')}",
            f"**Overall Signal:** [{signal_emoji}] {signal.upper()}",
            f"**Confidence Score:** {b.get('confidence_score', 'N/A')}/10",
            "",
            "## Bull Case",
        ]

        for i, point in enumerate(b.get("bull_case", []), 1):
            lines.append(f"{i}. {point}")

        lines.extend(["", "## Bear Case"])
        for i, point in enumerate(b.get("bear_case", []), 1):
            lines.append(f"{i}. {point}")

        lines.extend(["", "## Key Metrics to Watch"])
        for metric in b.get("key_metrics_to_watch", []):
            lines.append(f"- {metric}")

        lines.extend([
            "",
            "---",
            f"*Generated: {brief_row.get('created_at', 'N/A')}*",
        ])

        return "\n".join(lines)

    def close(self):
        """Close the database connection."""
        self.conn.close()
