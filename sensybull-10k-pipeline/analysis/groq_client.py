"""Groq API wrapper for analyzing 10-K filing sections."""

import asyncio
import json
import logging
import re

from groq import AsyncGroq, RateLimitError

from analysis.prompts import INVESTMENT_BRIEF_PROMPT, SECTION_PROMPTS
from config import config

logger = logging.getLogger(__name__)


class GroqAnalyzer:
    """Wraps the Groq API for structured analysis of 10-K sections."""

    def __init__(self):
        self.client = AsyncGroq(api_key=config.groq_api_key)
        self.model = config.groq_model
        self.max_words = config.max_section_words
        self.total_tokens_used = 0

    def _truncate_text(self, text: str) -> str:
        """Truncate text to max_words to fit within context limits."""
        words = text.split()
        if len(words) > self.max_words:
            logger.info("Truncating text from %d to %d words", len(words), self.max_words)
            return " ".join(words[: self.max_words])
        return text

    @staticmethod
    def _parse_json_response(text: str) -> dict:
        """Parse JSON from LLM response, handling markdown code fences."""
        cleaned = text.strip()

        # Strip markdown code fences (```json ... ``` or ``` ... ```)
        fence_pattern = re.compile(r"^```(?:json)?\s*\n?(.*?)\n?\s*```$", re.DOTALL)
        match = fence_pattern.match(cleaned)
        if match:
            cleaned = match.group(1).strip()

        # Try direct JSON parse
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            pass

        # Try to find a JSON object in the response
        brace_match = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if brace_match:
            try:
                return json.loads(brace_match.group(0))
            except json.JSONDecodeError:
                pass

        raise ValueError(f"Could not parse JSON from response: {cleaned[:200]}...")

    async def analyze_section(
        self, section_name: str, text: str, ticker: str
    ) -> dict:
        """Analyze a single 10-K section using the appropriate prompt template.

        Returns parsed JSON dict on success, or an error dict on failure.
        """
        if section_name not in SECTION_PROMPTS:
            return {"error": f"No prompt template for section: {section_name}"}

        truncated_text = self._truncate_text(text)
        prompt = SECTION_PROMPTS[section_name].format(ticker=ticker, text=truncated_text)

        for attempt in range(3):
            try:
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    max_tokens=2000,
                )

                # Log token usage
                usage = response.usage
                if usage:
                    tokens = usage.total_tokens
                    self.total_tokens_used += tokens
                    logger.info(
                        "[%s/%s] Tokens used: %d (prompt: %d, completion: %d, total session: %d)",
                        ticker, section_name, tokens,
                        usage.prompt_tokens, usage.completion_tokens,
                        self.total_tokens_used,
                    )

                raw_text = response.choices[0].message.content
                try:
                    result = self._parse_json_response(raw_text)
                    result["_section"] = section_name
                    result["_model"] = self.model
                    return result
                except ValueError as e:
                    logger.error(
                        "[%s/%s] JSON parse failed: %s", ticker, section_name, e
                    )
                    return {
                        "error": "parse_failed",
                        "raw_text": raw_text,
                        "_section": section_name,
                        "_model": self.model,
                    }

            except RateLimitError:
                wait = 2 ** (attempt + 1)
                logger.warning(
                    "[%s/%s] Rate limited, retrying in %ds (attempt %d/3)",
                    ticker, section_name, wait, attempt + 1,
                )
                await asyncio.sleep(wait)
            except Exception as e:
                logger.error(
                    "[%s/%s] Groq API error: %s", ticker, section_name, e
                )
                return {
                    "error": str(e),
                    "_section": section_name,
                    "_model": self.model,
                }

        return {
            "error": "max_retries_exceeded",
            "_section": section_name,
            "_model": self.model,
        }

    async def generate_investment_brief(
        self, analyses: dict, ticker: str, xbrl_facts: dict
    ) -> dict:
        """Synthesize all section analyses into a final investment brief.

        Returns parsed JSON dict on success, or an error dict on failure.
        """
        # Build context from analyses
        analyses_clean = {}
        for section_name, analysis in analyses.items():
            # Remove internal metadata keys for the prompt
            clean = {k: v for k, v in analysis.items() if not k.startswith("_")}
            analyses_clean[section_name] = clean

        # Format XBRL facts for the prompt
        xbrl_summary = {
            "company_name": xbrl_facts.get("company_name", "Unknown"),
            "revenue": xbrl_facts.get("revenue", []),
            "net_income": xbrl_facts.get("net_income", []),
            "total_assets": xbrl_facts.get("total_assets", []),
        }

        company_name = xbrl_facts.get("company_name", ticker)
        fiscal_year = ""
        if xbrl_facts.get("revenue"):
            fiscal_year = str(xbrl_facts["revenue"][0].get("fiscal_year", ""))

        prompt = INVESTMENT_BRIEF_PROMPT.format(
            ticker=ticker,
            company=company_name,
            fiscal_year=fiscal_year,
            analyses_json=json.dumps(analyses_clean, indent=2),
            xbrl_json=json.dumps(xbrl_summary, indent=2),
        )

        for attempt in range(3):
            try:
                response = await self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1,
                    max_tokens=2000,
                )

                usage = response.usage
                if usage:
                    tokens = usage.total_tokens
                    self.total_tokens_used += tokens
                    logger.info(
                        "[%s/brief] Tokens used: %d (total session: %d)",
                        ticker, tokens, self.total_tokens_used,
                    )

                raw_text = response.choices[0].message.content
                try:
                    result = self._parse_json_response(raw_text)
                    result["_model"] = self.model
                    return result
                except ValueError as e:
                    logger.error("[%s/brief] JSON parse failed: %s", ticker, e)
                    return {
                        "error": "parse_failed",
                        "raw_text": raw_text,
                        "_model": self.model,
                    }

            except RateLimitError:
                wait = 2 ** (attempt + 1)
                logger.warning(
                    "[%s/brief] Rate limited, retrying in %ds (attempt %d/3)",
                    ticker, wait, attempt + 1,
                )
                await asyncio.sleep(wait)
            except Exception as e:
                logger.error("[%s/brief] Groq API error: %s", ticker, e)
                return {"error": str(e), "_model": self.model}

        return {"error": "max_retries_exceeded", "_model": self.model}
