"""Exact, bounded source context for private evidence inspection, not entailment."""
import re

from .investigation import _version

MAX_CONTEXT_BYTES = 2400


def citation_context(text: str, start: int, end: int, quote: str) -> dict:
    """Show adjacent sentence-sized spans within a paragraph; never repair a quote.

    Punctuation boundaries are display heuristics, not linguistic guarantees.
    An oversized window is omitted whole, never silently clipped. Offsets always
    refer to the unchanged source, including Unicode and original whitespace.
    """
    if (type(text) is not str or type(quote) is not str or not quote
            or type(start) is not int or type(end) is not int
            or not 0 <= start < end <= len(text) or text[start:end] != quote):
        raise ValueError("invalid_passage_citation")
    # Blank lines are a hard boundary so roundup paragraphs cannot bleed together.
    gaps = list(re.finditer(r'\n[ \t\r]*\n', text))
    left = max((m.end() for m in gaps if m.end() <= start), default=0)
    right = min((m.start() for m in gaps if m.start() >= end), default=len(text))
    if any(start < m.start() < end for m in gaps):
        return _window(text, start, end, start, end, "cross_paragraph_quote")
    boundaries = [left]
    boundaries.extend(left + m.end() for m in re.finditer(
        r'[.!?][\u201d\u2019\"]?\s+|\n', text[left:right]))
    boundaries.append(right)
    boundaries = sorted(set(boundaries))
    first = max(i for i, p in enumerate(boundaries) if p <= start)
    last = next(i for i, p in enumerate(boundaries) if p >= end)
    a, b = boundaries[max(0, first - 1)], boundaries[min(len(boundaries) - 1, last + 1)]
    if len(text[a:b].encode("utf-8")) > MAX_CONTEXT_BYTES:
        return _window(text, start, end, start, end, "context_over_budget")
    return _window(text, start, end, a, b, "available")


def _window(text, start, end, a, b, status):
    return {"source_version": _version(text), "start": a, "end": b,
            "citation_start": start, "citation_end": end,
            "text": text[a:b], "status": status, "public_eligible": False}
