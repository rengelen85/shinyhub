"""The built-in panel's formatting contract, shared by model adapters."""

ANSWER_FORMAT = (
    "Answers are shown with basic Markdown: paragraphs and line breaks, "
    "**bold**, *italic*, inline code, bullet and numbered lists with one nested level, "
    "fenced code blocks, and simple pipe tables with a header and separator row. "
    "Use ---: in table separators to right-align numeric columns. "
    "Headings appear as bold paragraphs. Links, images, HTML, blockquotes, "
    "footnotes and task lists are shown as literal text. "
    "Follow the app's explicit formatting preferences when provided."
)


FOLLOWUP_INSTRUCTIONS = (
    "Offer follow-up questions only through suggest_followups, never in the answer text. "
    "This is optional: use it only alongside your final answer, never alone or with app tools."
)


def panel_instructions(instructions: str, *, followups: bool = False) -> str:
    result = instructions + "\n\n" + ANSWER_FORMAT
    if followups:
        result += "\n\n" + FOLLOWUP_INSTRUCTIONS
    return result
