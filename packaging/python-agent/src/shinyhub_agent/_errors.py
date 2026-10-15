"""Fixed visitor messages for provider failures; never display upstream text."""

from typing import ClassVar

from ._core import ToolError


class ProviderError(ToolError):
    MESSAGES: ClassVar[dict[str, str]] = {
        "budget_exceeded": "The assistant budget is used up. Contact your administrator.",
        "end_user_required": (
            "The assistant needs a verified user identity. "
            "Reopen the app or contact your administrator."
        ),
    }

    def __init__(self, code: str):
        super().__init__(code, self.MESSAGES[code])
