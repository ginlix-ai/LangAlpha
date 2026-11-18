"""Form vocabulary: the items a form has in a given era, and their titles."""
from __future__ import annotations

from .sections.vocab import EIGHTK_NEW_FROM, form_family, spec_for

__all__ = ["item_order", "item_title", "mdna_item"]

TITLES: dict[str, dict[str, str]] = {
    "10-K": {
        "1": "Business", "1A": "Risk Factors", "1B": "Unresolved Staff Comments", "1C": "Cybersecurity",
        "2": "Properties", "3": "Legal Proceedings", "4": "Mine Safety Disclosures",
        "5": "Market for Registrant's Common Equity, Related Stockholder Matters and Issuer Purchases of Equity Securities",
        "6": "Reserved", "7": "Management's Discussion and Analysis of Financial Condition and Results of Operations",
        "7A": "Quantitative and Qualitative Disclosures About Market Risk",
        "8": "Financial Statements and Supplementary Data",
        "9": "Changes in and Disagreements with Accountants on Accounting and Financial Disclosure",
        "9A": "Controls and Procedures", "9B": "Other Information",
        "9C": "Disclosure Regarding Foreign Jurisdictions that Prevent Inspections",
        "10": "Directors, Executive Officers and Corporate Governance", "11": "Executive Compensation",
        "12": "Security Ownership of Certain Beneficial Owners and Management and Related Stockholder Matters",
        "13": "Certain Relationships and Related Transactions, and Director Independence",
        "14": "Principal Accountant Fees and Services", "15": "Exhibits and Financial Statement Schedules",
        "16": "Form 10-K Summary",
    },
    "10-Q": {
        "I-1": "Financial Statements",
        "I-2": "Management's Discussion and Analysis of Financial Condition and Results of Operations",
        "I-3": "Quantitative and Qualitative Disclosures About Market Risk", "I-4": "Controls and Procedures",
        "II-1": "Legal Proceedings", "II-1A": "Risk Factors",
        "II-2": "Unregistered Sales of Equity Securities and Use of Proceeds", "II-3": "Defaults Upon Senior Securities",
        "II-4": "Mine Safety Disclosures", "II-5": "Other Information", "II-6": "Exhibits",
    },
    "20-F": {
        "1": "Identity of Directors, Senior Management and Advisers", "2": "Offer Statistics and Expected Timetable",
        "3": "Key Information", "4": "Information on the Company", "4A": "Unresolved Staff Comments",
        "5": "Operating and Financial Review and Prospects", "6": "Directors, Senior Management and Employees",
        "7": "Major Shareholders and Related Party Transactions", "8": "Financial Information",
        "9": "The Offer and Listing", "10": "Additional Information",
        "11": "Quantitative and Qualitative Disclosures About Market Risk",
        "12": "Description of Securities Other Than Equity Securities",
        "13": "Defaults, Dividend Arrearages and Delinquencies",
        "14": "Material Modifications to the Rights of Security Holders and Use of Proceeds",
        "15": "Controls and Procedures", "16": "Reserved", "16A": "Audit Committee Financial Expert",
        "16B": "Code of Ethics", "16C": "Principal Accountant Fees and Services",
        "16D": "Exemptions from the Listing Standards for Audit Committees",
        "16E": "Purchases of Equity Securities by the Issuer and Affiliated Purchasers",
        "16F": "Change in Registrant's Certifying Accountant", "16G": "Corporate Governance",
        "16H": "Mine Safety Disclosure", "16I": "Disclosure Regarding Foreign Jurisdictions that Prevent Inspections",
        "16J": "Insider Trading Policies", "16K": "Cybersecurity", "17": "Financial Statements",
        "18": "Financial Statements", "19": "Exhibits",
    },
    "40-F": {
        "AIF": "Annual Information Form", "AFS": "Audited Annual Financial Statements",
        "MDA": "Management's Discussion and Analysis", "DCP": "Disclosure Controls and Procedures",
        "AC": "Audit Committee", "ACFE": "Audit Committee Financial Expert", "CODE": "Code of Ethics",
        "FEES": "Principal Accountant Fees and Services", "OBS": "Off-Balance Sheet Arrangements",
        "CONTRACTUAL": "Contractual Obligations", "MINE": "Mine Safety Disclosure", "CYBER": "Cybersecurity",
        "CYBERSECURITY": "Cybersecurity", "INSIDER": "Insider Trading Policies", "RECOVERY": "Recovery of Erroneously Awarded Compensation",
        "NYSE": "NYSE Corporate Governance", "NYSE_GOV": "NYSE Corporate Governance",
        "UNDERTAKING": "Undertaking", "CONSENT": "Consent to Service of Process",
    },
    "8-K": {
        "1.01": "Entry into a Material Definitive Agreement", "1.02": "Termination of a Material Definitive Agreement",
        "1.03": "Bankruptcy or Receivership", "1.04": "Mine Safety: Reporting of Shutdowns and Patterns of Violations",
        "1.05": "Material Cybersecurity Incidents",
        "2.01": "Completion of Acquisition or Disposition of Assets", "2.02": "Results of Operations and Financial Condition",
        "2.03": "Creation of a Direct Financial Obligation", "2.04": "Triggering Events That Accelerate Obligations",
        "2.05": "Costs Associated with Exit or Disposal Activities", "2.06": "Material Impairments",
        "3.01": "Notice of Delisting or Failure to Satisfy a Listing Rule", "3.02": "Unregistered Sales of Equity Securities",
        "3.03": "Material Modification to Rights of Security Holders",
        "4.01": "Changes in Registrant's Certifying Accountant",
        "4.02": "Non-Reliance on Previously Issued Financial Statements",
        "5.01": "Changes in Control of Registrant", "5.02": "Departure or Election of Directors or Officers",
        "5.03": "Amendments to Articles of Incorporation or Bylaws",
        "5.04": "Temporary Suspension of Trading Under Employee Plans", "5.05": "Amendment to Code of Ethics",
        "5.06": "Change in Shell Company Status", "5.07": "Submission of Matters to a Vote of Security Holders",
        "5.08": "Shareholder Nominations", "6.01": "ABS Informational and Computational Material",
        "6.02": "Change of Servicer or Trustee", "6.03": "Change in Credit Enhancement",
        "6.04": "Failure to Make a Required Distribution", "6.05": "Securities Act Updating Disclosure",
        "7.01": "Regulation FD Disclosure", "8.01": "Other Events", "9.01": "Financial Statements and Exhibits",
    },
}

#: The first filing date by which every filer gives an item its title above. An item that meant something else
#: before (10-K Item 4 was the vote of security holders until 2010, Item 6 selected financial data until 2021, Item 14
#: the exhibit list until 2003) has no title from the table before it, so a caller shows the heading the filer printed.
#: "*" dates a whole form: the 20-F was renumbered in 2000 and the 8-K in 2004, so none of their items has a title
#: from the table before.
TITLED_SINCE: dict[tuple[str, str], str] = {
    ("10-K", "4"): "2012-02-01", ("10-K", "6"): "2021-11-01", ("10-K", "14"): "2004-06-01",
    ("10-K", "15"): "2004-06-01", ("10-Q", "II-4"): "2012-02-01", ("20-F", "16"): "2004-06-01",
    ("20-F", "*"): "2001-10-01", ("8-K", "*"): EIGHTK_NEW_FROM,
}
#: The first filing date by which every filer's form has an item added after EDGAR began: the adopting release's
#: effective date, or later where filers phased in (1C and 16K from fiscal years ending on or after 2023-12-15). An
#: item is never missing from a filing made before its form had it.
ADDED_SINCE: dict[tuple[str, str], str] = {
    ("10-K", "7A"): "1998-06-15", ("10-K", "9A"): "2003-08-14", ("10-K", "9B"): "2004-08-23",
    ("10-K", "1A"): "2005-12-01", ("10-K", "1B"): "2005-12-01", ("10-K", "16"): "2016-06-09",
    ("10-K", "9C"): "2022-01-10", ("10-K", "1C"): "2023-12-15",
    ("10-Q", "I-3"): "1998-06-15", ("10-Q", "I-4"): "2003-08-14", ("10-Q", "II-1A"): "2005-12-01",
    ("20-F", "4A"): "2005-12-01", ("20-F", "16A"): "2004-06-01", ("20-F", "16B"): "2004-06-01",
    ("20-F", "16C"): "2004-06-01", ("20-F", "16D"): "2004-06-01", ("20-F", "16E"): "2004-06-01",
    ("20-F", "16F"): "2009-01-01", ("20-F", "16G"): "2009-01-01", ("20-F", "16H"): "2012-02-01",
    ("20-F", "16I"): "2022-01-10", ("20-F", "16K"): "2023-12-15",
    ("20-F", "16J"): "2024-04-01",
}
MDNA = {"10-K": "7", "10-Q": "I-2", "20-F": "5", "40-F": "MDA"}


def item_order(form: str, filing_date: str | None = None) -> tuple[str, ...]:
    """The items form prints for a filing made on filing_date (None: today), in form order, leaving out the ones it
    did not have yet (ADDED_SINCE); empty for a 40-F, whose sections are found by their titles and come in no fixed
    order."""
    family, filed = form_family(form), filing_date or "9999"
    return tuple(i for i in spec_for(form, filing_date).order if filed >= ADDED_SINCE.get((family, i), ""))


def item_title(form: str, item: str, filing_date: str | None = None) -> str | None:
    """The title form gives item, for a filing made on filing_date (None: today). None when the form has no such
    item, or the item had another meaning then (TITLED_SINCE)."""
    try:
        family = form_family(form)
    except ValueError:
        return None
    since = max(TITLED_SINCE.get((family, item), ""), TITLED_SINCE.get((family, "*"), ""))
    if since and (filing_date or "9999") < since:
        return None
    return TITLES.get(family, {}).get(item)


def mdna_item(form: str) -> str | None:
    """The item holding management's discussion and analysis."""
    try:
        return MDNA.get(form_family(form))
    except ValueError:
        return None
