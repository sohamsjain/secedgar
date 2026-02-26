"""Prompt templates for Groq LLM analysis of 10-K filing sections."""

BUSINESS_PROMPT = """You are a senior equity research analyst. Analyze the following Business section (Item 1) from a 10-K filing for {ticker}.

Extract the following information and return ONLY a valid JSON object with these keys:
- "business_model": A concise description of how the company makes money (2-3 sentences)
- "primary_revenue_streams": A list of the main revenue streams/segments
- "competitive_moat": Description of the company's competitive advantages and barriers to entry
- "key_markets": The primary geographic and industry markets served
- "one_line_summary": A single sentence summarizing the company's business

IMPORTANT: Return ONLY the JSON object, no additional text or markdown formatting.

Business Section Text:
{text}"""

RISK_FACTORS_PROMPT = """You are a senior equity research analyst specializing in risk assessment. Analyze the following Risk Factors section (Item 1A) from a 10-K filing for {ticker}.

Extract the following information and return ONLY a valid JSON object with these keys:
- "top_5_risks": A list of the 5 most material risks, each as an object with:
  - "risk_name": Short name for the risk
  - "description": 1-2 sentence description
  - "severity": "high", "medium", or "low"
- "novel_risks": A list of risks that are unusual or not typically seen in similar companies in the same sector
- "management_concern_level": An integer from 1-10 indicating how concerned management appears about the risk environment (10 = extremely concerned)

IMPORTANT: Return ONLY the JSON object, no additional text or markdown formatting.

Risk Factors Section Text:
{text}"""

MDA_PROMPT = """You are a senior equity research analyst. Analyze the following Management's Discussion and Analysis (MD&A, Item 7) section from a 10-K filing for {ticker}.

Extract the following information and return ONLY a valid JSON object with these keys:
- "revenue_trend": Description of revenue trends and growth trajectory
- "margin_commentary": Analysis of profit margins and their direction
- "management_tone": One of "bullish", "neutral", or "cautious" - reflecting management's overall tone
- "key_drivers": A list of the primary business drivers discussed
- "red_flags": A list of any concerning items or negative trends mentioned
- "forward_looking_signals": A list of forward-looking statements or guidance signals

IMPORTANT: Return ONLY the JSON object, no additional text or markdown formatting.

MD&A Section Text:
{text}"""

INVESTMENT_BRIEF_PROMPT = """You are a senior equity research analyst preparing an investment brief. Using the following analyses of a 10-K filing for {ticker} ({company}), fiscal year {fiscal_year}, synthesize a comprehensive investment brief.

Section Analyses:
{analyses_json}

Key Financial Metrics (from XBRL):
{xbrl_json}

Return ONLY a valid JSON object with these keys:
- "company": Full company name
- "ticker": Stock ticker symbol
- "fiscal_year": The fiscal year of this filing
- "bull_case": A list of exactly 3 bullish points supporting investment
- "bear_case": A list of exactly 3 bearish points or risks
- "key_metrics_to_watch": A list of specific metrics investors should monitor going forward
- "overall_signal": One of "positive", "neutral", or "negative"
- "confidence_score": An integer from 1-10 indicating confidence in this assessment

IMPORTANT: Return ONLY the JSON object, no additional text or markdown formatting."""

# Map section names to their prompt templates
SECTION_PROMPTS = {
    "business": BUSINESS_PROMPT,
    "risk_factors": RISK_FACTORS_PROMPT,
    "mda": MDA_PROMPT,
}
