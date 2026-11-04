"""
SEC filing parser using edgartools library.

This parser uses edgartools for direct SEC EDGAR access with structured
section extraction, financial statements, and multi-year comparison data.
"""

import logging
from typing import Any, Dict, List, Optional


from .base import BaseSECParser, ParsingFailedError
from ..types import SECSection, FilingType

logger = logging.getLogger(__name__)


# Patterns for detecting row types (case-insensitive)
TOTAL_PATTERNS = [
    r"^total\s",  # "Total assets", "Total liabilities"
    r"\stotal$",  # "Net income total"
    r"^net\s(?:income|loss|sales|revenue|cash)",  # "Net income", "Net sales"
]
SUBTOTAL_PATTERNS = [
    r"^total\s(?:current|non-current|operating)",  # "Total current assets"
    r"(?:current|non-current)\s.*total$",
]

# Identity for SEC EDGAR access (required by SEC)
SEC_IDENTITY = "OpenSource user@example.com"


class EdgarToolsParser(BaseSECParser):
    """
    Parser using edgartools for direct SEC EDGAR access.

    Features:
    - Direct SEC access (no API key required)
    - Structured TenK/TenQ objects with section accessors
    - Financial statements as formatted tables
    - Multi-year comparison data (YoY built-in)
    - Financial metrics extraction
    """

    def __init__(self):
        """Initialize edgartools with SEC identity."""
        try:
            from edgar import set_identity

            set_identity(SEC_IDENTITY)
            self._initialized = True
        except ImportError:
            logger.warning("edgartools not installed")
            self._initialized = False

    @property
    def name(self) -> str:
        return "edgartools"

    def supports_filing_type(self, filing_type: FilingType) -> bool:
        """Supports both 10-K and 10-Q."""
        return filing_type in (FilingType.FORM_10K, FilingType.FORM_10Q)

    def get_latest_filing(
        self, symbol: str, filing_type: FilingType
    ) -> Optional[Any]:
        """
        Get the latest filing for a symbol.

        Args:
            symbol: Stock ticker symbol
            filing_type: Type of filing (10-K or 10-Q)

        Returns:
            Filing object or None if not found
        """
        if not self._initialized:
            raise ParsingFailedError("edgartools not initialized")

        try:
            from edgar import Company

            company = Company(symbol)
            form = filing_type.value  # "10-K" or "10-Q"
            # Exclude amendments (10-K/A, 10-Q/A) to get original filings with full XBRL data
            filings = company.get_filings(form=form, amendments=False)

            if not filings or len(filings) == 0:
                return None

            return filings.latest()

        except Exception as e:
            logger.warning(f"Failed to get filing for {symbol}: {e}")
            return None

    def parse(
        self,
        html: str,
        filing_type: FilingType,
        sections: Optional[List[str]] = None,
    ) -> Dict[str, SECSection]:
        """
        Parse is not used directly - use parse_filing instead.

        This method exists for interface compatibility.
        """
        raise NotImplementedError(
            "EdgarToolsParser.parse() not supported. Use parse_filing() instead."
        )

    def parse_filing(
        self,
        symbol: str,
        filing_type: FilingType,
        sections: Optional[List[str]] = None,
        include_financials: bool = True,
        output_format: str = "markdown",
    ) -> Dict[str, Any]:
        """
        Parse SEC filing using edgartools.

        Args:
            symbol: Stock ticker symbol
            filing_type: Type of filing (10-K or 10-Q)
            sections: Optional list of sections to extract
            include_financials: Include financial statements and metrics
            output_format: "markdown" (default) or "dict"

        Returns:
            Dictionary with filing data including sections and financials
        """
        filing = self.get_latest_filing(symbol, filing_type)
        if not filing:
            raise ParsingFailedError(f"No {filing_type.value} found for {symbol}")

        try:
            # Get structured object (TenK or TenQ)
            obj = filing.obj()

            result: Dict[str, Any] = {
                "symbol": symbol.upper(),
                "filing_type": filing_type.value,
                "filing_date": str(filing.filing_date),
                "period_end": str(filing.period_of_report) if filing.period_of_report else None,
                "cik": str(filing.cik),
                "source_url": filing.filing_url,
                "sections": {},
                "financial_statements": {},
                "financial_metrics": {},
            }

            # Extract text sections
            if filing_type == FilingType.FORM_10K:
                result["sections"] = self._extract_10k_sections(obj, sections)
            else:
                result["sections"] = self._extract_10q_sections(filing, obj, sections)

            # Extract financial statements and metrics
            if include_financials:
                result["financial_statements"] = self._extract_financial_statements(obj)
                result["financial_metrics"] = self._extract_financial_metrics(obj)

            # Calculate totals
            result["total_content_length"] = sum(
                s.get("length", 0) for s in result["sections"].values()
            )
            result["sections_extracted"] = len(result["sections"])

            logger.debug(
                f"edgartools extracted {result['sections_extracted']} sections "
                f"from {filing_type.value} for {symbol}"
            )

            # Convert to markdown if requested
            if output_format == "markdown":
                markdown = self._format_as_markdown(result)
                metadata = {
                    "symbol": result["symbol"],
                    "filing_type": result["filing_type"],
                    "filing_date": result["filing_date"],
                    "period_end": result["period_end"],
                    "cik": result["cik"],
                    "source_url": result["source_url"],
                    "sections_extracted": result["sections_extracted"],
                }
                return {"content": markdown, "metadata": metadata}

            return result

        except ParsingFailedError:
            raise
        except Exception as e:
            logger.warning(f"edgartools parsing failed: {e}")
            raise ParsingFailedError(f"edgartools parsing failed: {e}")

    def _format_as_markdown(self, result: Dict[str, Any]) -> str:
        """Format the result as markdown string."""
        md_parts = []

        # Header
        md_parts.append(f"# {result['symbol']} {result['filing_type']} Filing\n")
        md_parts.append(f"**Filing Date:** {result['filing_date']}")
        md_parts.append(f"**Period End:** {result['period_end']}")
        md_parts.append(f"**CIK:** {result['cik']}")
        md_parts.append(f"**Source:** {result['source_url']}")
        md_parts.append("")
        md_parts.append("> **IMPORTANT:** When using information from this SEC filing, always cite the source URL above in your response.\n")

        # Financial metrics summary
        metrics = result.get("financial_metrics", {})
        if metrics:
            md_parts.append("## Key Financial Metrics\n")
            md_parts.append("| Metric | Value |")
            md_parts.append("|--------|-------|")
            if metrics.get("revenue"):
                md_parts.append(f"| Revenue | ${metrics['revenue']/1e9:.2f}B |")
            if metrics.get("net_income"):
                md_parts.append(f"| Net Income | ${metrics['net_income']/1e9:.2f}B |")
            if metrics.get("current_ratio"):
                md_parts.append(f"| Current Ratio | {metrics['current_ratio']:.2f} |")
            md_parts.append("")

        # Financial statements (already in markdown table format)
        statements = result.get("financial_statements", {})
        if statements:
            md_parts.append("## Financial Statements\n")
            for _, stmt_content in statements.items():
                # Content already has title from to_markdown()
                md_parts.append(stmt_content)
                md_parts.append("")

        # Sections
        sections = result.get("sections", {})
        if sections:
            md_parts.append("## Filing Sections\n")
            for _, section_data in sections.items():
                md_parts.append(f"### {section_data['title']}\n")
                md_parts.append(f"*Length: {section_data['length']:,} characters*\n")
                md_parts.append(section_data["content"])
                md_parts.append("\n---\n")

        # Return markdown string directly
        return "\n".join(md_parts)

    def _extract_10k_sections(
        self, tenk: Any, sections: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """Extract sections from TenK object."""
        result = {}

        # Map of section keys to TenK attributes
        section_map = {
            "item_1": ("business", "Item 1 - Business"),
            "item_1a": ("risk_factors", "Item 1A - Risk Factors"),
            "item_7": ("management_discussion", "Item 7 - MD&A"),
            "item_10": ("directors_officers_and_governance", "Item 10 - Directors & Officers"),
        }

        for key, (attr, title) in section_map.items():
            if sections and key not in sections:
                continue

            try:
                content = getattr(tenk, attr, None)
                if content and isinstance(content, str) and len(content) > 0:
                    result[key] = {
                        "title": title,
                        "content": content,
                        "length": len(content),
                    }
                    logger.debug(f"Extracted {key}: {len(content):,} chars")
            except Exception as e:
                logger.debug(f"Failed to extract {key}: {e}")

        return result

    def _extract_10q_sections(
        self, filing: Any, tenq: Any, sections: Optional[List[str]] = None
    ) -> Dict[str, Dict[str, Any]]:
        """
        Extract sections from TenQ using indexed access.

        TenQ uses indexed access like tenq['Part I, Item 2'] instead of
        property accessors like TenK.
        """
        result = {}

        # Map section keys to TenQ indexed access keys
        section_map = {
            "part1_item1": ("Part I, Item 1", "Part I Item 1 - Financial Statements"),
            "part1_item2": ("Part I, Item 2", "Part I Item 2 - MD&A"),
            "part1_item3": ("Part I, Item 3", "Part I Item 3 - Quantitative Disclosures"),
            "part1_item4": ("Part I, Item 4", "Part I Item 4 - Controls and Procedures"),
            "part2_item1": ("Part II, Item 1", "Part II Item 1 - Legal Proceedings"),
            "part2_item1a": ("Part II, Item 1A", "Part II Item 1A - Risk Factors"),
            "part2_item2": ("Part II, Item 2", "Part II Item 2 - Unregistered Sales"),
            "part2_item5": ("Part II, Item 5", "Part II Item 5 - Other Information"),
            "part2_item6": ("Part II, Item 6", "Part II Item 6 - Exhibits"),
        }

        # Determine which sections to extract
        if sections:
            target_sections = {k: v for k, v in section_map.items() if k in sections}
        else:
            # Default: MD&A and Risk Factors
            target_sections = {
                "part1_item2": section_map["part1_item2"],
                "part2_item1a": section_map["part2_item1a"],
            }

        # Extract each section using TenQ indexed access
        for key, (index_key, title) in target_sections.items():
            try:
                content = tenq[index_key]
                if content and isinstance(content, str) and len(content) > 0:
                    result[key] = {
                        "title": title,
                        "content": content,
                        "length": len(content),
                    }
                    logger.debug(f"Extracted {key}: {len(content):,} chars")
            except (KeyError, IndexError) as e:
                logger.debug(f"Section {index_key} not found: {e}")
            except Exception as e:
                logger.debug(f"Failed to extract {key}: {e}")

        # Include full text if requested or as fallback
        if not result or "full_text" in (sections or []):
            try:
                full_text = filing.text()
                if full_text:
                    result["full_text"] = {
                        "title": "Full Filing Text",
                        "content": full_text,
                        "length": len(full_text),
                    }
            except Exception as e:
                logger.debug(f"Failed to get full text: {e}")

        logger.debug(f"Extracted {len(result)} sections from 10-Q")
        return result

    def _extract_financial_statements(self, obj: Any) -> Dict[str, str]:
        """Extract financial statements as markdown tables."""
        statements = {}

        # Access statements via financials property
        financials = getattr(obj, "financials", None)
        if not financials:
            return statements

        statement_methods = [
            ("balance_sheet", "balance_sheet"),
            ("income_statement", "income_statement"),
            ("cash_flow_statement", "cash_flow_statement"),
        ]

        for key, method_name in statement_methods:
            try:
                method = getattr(financials, method_name, None)
                if method:
                    stmt = method()
                    if stmt:
                        # Use native to_markdown() for cleaner output
                        rendered = stmt.render()
                        if hasattr(rendered, "to_markdown"):
                            text = rendered.to_markdown()
                        else:
                            text = str(rendered)
                        if text and len(text) > 0:
                            statements[key] = text
                            logger.debug(f"Extracted {key}: {len(text):,} chars")
            except Exception as e:
                logger.debug(f"Failed to extract {key}: {e}")

        return statements

    def _extract_financial_metrics(self, obj: Any) -> Dict[str, Any]:
        """Extract key financial metrics."""
        metrics = {}

        try:
            financials = getattr(obj, "financials", None)
            if financials and hasattr(financials, "get_financial_metrics"):
                metrics = financials.get_financial_metrics()
                logger.debug(f"Extracted {len(metrics)} financial metrics")
        except Exception as e:
            logger.debug(f"Failed to extract financial metrics: {e}")

        return metrics

    def get_filing_text(self, symbol: str, filing_type: FilingType) -> Optional[str]:
        """Get full filing text."""
        filing = self.get_latest_filing(symbol, filing_type)
        if filing:
            try:
                return filing.text()
            except Exception as e:
                logger.warning(f"Failed to get filing text: {e}")
        return None

    def get_filing_markdown(self, symbol: str, filing_type: FilingType) -> Optional[str]:
        """Get full filing as markdown."""
        filing = self.get_latest_filing(symbol, filing_type)
        if filing:
            try:
                return filing.markdown()
            except Exception as e:
                logger.warning(f"Failed to get filing markdown: {e}")
        return None
