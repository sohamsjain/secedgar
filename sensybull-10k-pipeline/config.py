"""Configuration management for the 10-K analysis pipeline."""

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    groq_api_key: str = field(default_factory=lambda: os.getenv("GROQ_API_KEY", ""))
    sec_user_agent: str = field(
        default_factory=lambda: os.getenv("SEC_USER_AGENT", "sensybull-pipeline admin@example.com")
    )
    poll_interval: int = field(
        default_factory=lambda: int(os.getenv("POLL_INTERVAL", "300"))
    )
    db_path: str = field(
        default_factory=lambda: os.getenv("DB_PATH", "sensybull_10k.db")
    )
    groq_model: str = "llama-3.3-70b-versatile"
    max_section_words: int = 6000
    sec_rate_limit_delay: float = 0.12  # ~8 req/sec to stay under 10/sec

    # Default watchlist: ticker -> CIK
    watchlist: dict = field(default_factory=lambda: {
        "AAPL": "320193",
        "MSFT": "789019",
        "GOOGL": "1652044",
        "NVDA": "1045810",
        "META": "1326801",
    })

    def validate(self) -> list[str]:
        """Return a list of configuration errors, empty if valid."""
        errors = []
        if not self.groq_api_key:
            errors.append("GROQ_API_KEY is not set")
        if not self.sec_user_agent or self.sec_user_agent == "sensybull-pipeline admin@example.com":
            errors.append("SEC_USER_AGENT should be set to your name and email")
        return errors


config = Config()
