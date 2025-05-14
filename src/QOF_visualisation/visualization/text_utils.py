"""Text utilities for QOF visualization."""

import textwrap


def md_wrap(text: str | None, width: int = 80) -> str:
    """
    Wrap text to specified width with line breaks.

    Args:
        text: The text to wrap. If None, returns empty string.
        width: Maximum line width for wrapping.

    Returns:
        Wrapped text with markdown-compatible line breaks.
    """
    if not text:
        return ""
    return "  \n".join(textwrap.wrap(text, width))
