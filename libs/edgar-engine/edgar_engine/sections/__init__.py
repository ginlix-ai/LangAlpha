"""Filing sections: split 10-K, 10-Q, 8-K, 20-F and 40-F filings into their items.

```python
from edgar_engine.sections import extract_sections
sections = extract_sections(submission_text)
sections["7"].text
```

extract_sections, sections_for_filing and the model types are the package's API; its other modules are internal.
"""
from .extract import extract_sections, sections_for_filing
from .model import FilingSections, Section, Span, Target
from .version import SECTIONS_VERSION

__all__ = ["FilingSections", "SECTIONS_VERSION", "Section", "Span", "Target", "extract_sections", "sections_for_filing"]
