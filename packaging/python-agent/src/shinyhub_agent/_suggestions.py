"""Bounded, non-executable panel metadata shared by chat backends."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Sequence
from typing import Any

from ._runtime import ToolCall, bounded_int

FOLLOWUP_TOOL = "suggest_followups"


def question_text(value: Any, maximum: int) -> str | None:
    if not isinstance(value, str):
        return None
    # Reject line breaks before trimming, including Unicode line separators.
    if any(
        unicodedata.category(c) in ("Cc", "Cs", "Zl", "Zp")
        or c in "\u202a\u202b\u202c\u202d\u202e\u2066\u2067\u2068\u2069"
        for c in value
    ):
        return None
    value = value.strip()
    if not 1 <= len(value) <= maximum or re.search(r"</?[A-Za-z][^>]*>", value):
        return None
    return value


def validate_starters(values: Sequence[str] | None) -> list[str] | None:
    if values is None:
        return None
    if isinstance(values, (str, bytes)) or not isinstance(values, Sequence):
        raise TypeError("starters must be a sequence of strings or None")
    if len(values) > 4:
        raise ValueError("starters must contain at most 4 questions")
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        question = question_text(value, 120)
        if question is None:
            raise ValueError(
                "starters must contain plain-text questions of 1 to 120 characters without newlines"
            )
        if question.casefold() in seen:
            raise ValueError("starters must not contain duplicate questions")
        seen.add(question.casefold())
        result.append(question)
    return result


def clean_followups(values: Any, maximum: int) -> list[str]:
    if not maximum or not isinstance(values, list):
        return []
    result: list[str] = []
    seen: set[str] = set()
    for value in values[:64]:
        question = question_text(value, 100)
        if question is not None and question.casefold() not in seen:
            seen.add(question.casefold())
            result.append(question)
            if len(result) == maximum:
                break
    return result


def followup_spec(maximum: int) -> dict[str, Any]:
    bounded_int("followups", maximum, 1, 3)
    return {
        "name": FOLLOWUP_TOOL,
        "description": f"Offer up to {maximum} short follow-up questions the user can edit and send. "
        "Phrase each as a standalone question the user would ask. Do not repeat them in the answer.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "questions": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 100,
                    },
                    "maxItems": maximum,
                }
            },
            "required": ["questions"],
            "additionalProperties": False,
        },
    }


def terminal_followups(
    calls: Sequence[ToolCall], has_text: bool, maximum: int
) -> list[str]:
    if not maximum or not has_text or len(calls) != 1 or calls[0].name != FOLLOWUP_TOOL:
        return []
    arguments = calls[0].arguments
    if not isinstance(arguments, dict) or set(arguments) != {"questions"}:
        return []
    return clean_followups(arguments["questions"], maximum)
