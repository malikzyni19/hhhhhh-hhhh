"""Phase SIG-1: Telegram delivery for Live Monitor signal alerts.

Sends setup alerts to a user's own Telegram bot. Message delivery only — this
module never places orders, never reads market data, and never touches an
exchange. The only credential it handles is a Telegram bot token.

Design notes:
  - HTML parse mode, not MarkdownV2. MarkdownV2 requires escaping a long list
    of characters that appear routinely in symbols and prices, and a single
    missed one causes a silent 400 from Telegram. HTML needs only three.
  - Nothing here raises. Every function returns a result dict, so a delivery
    failure can never take down the funnel that called it.
"""
from __future__ import annotations

import re

import requests

_API_BASE  = "https://api.telegram.org"
_TIMEOUT   = 12
_MAX_LEN   = 4096          # Telegram's hard message length limit


def escape_html(text) -> str:
    """Escape the three characters Telegram's HTML parse mode reserves."""
    if text is None:
        return ""
    return (str(text)
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))


def _valid_token_shape(token: str) -> bool:
    """Cheap client-side sanity check: '<digits>:<35+ chars>'.

    Catches paste errors before spending a network round-trip on them.
    """
    return bool(re.fullmatch(r"\d{6,}:[A-Za-z0-9_\-]{30,}", (token or "").strip()))


def _post(token: str, method: str, payload: dict) -> dict:
    """POST to one Telegram Bot API method. Never raises."""
    url = f"{_API_BASE}/bot{token.strip()}/{method}"
    try:
        resp = requests.post(url, json=payload, timeout=_TIMEOUT)
    except requests.exceptions.Timeout:
        return {"ok": False, "error": "timeout",
                "message": "Telegram did not respond in time."}
    except requests.exceptions.RequestException as exc:
        return {"ok": False, "error": "network",
                "message": f"Could not reach Telegram: {str(exc)[:120]}"}

    try:
        body = resp.json()
    except ValueError:
        return {"ok": False, "error": "bad_response",
                "message": f"Telegram returned a non-JSON response (HTTP {resp.status_code})."}

    if not body.get("ok"):
        desc = str(body.get("description") or "unknown error")
        return {"ok": False,
                "error": "telegram_error",
                "error_code": body.get("error_code"),
                "message": _friendly_error(body.get("error_code"), desc)}

    return {"ok": True, "result": body.get("result") or {}}


def _friendly_error(code, description: str) -> str:
    """Turn Telegram's terse errors into something a user can act on."""
    d = (description or "").lower()
    if code == 401 or "unauthorized" in d:
        return ("Bot token is not valid. Copy it again from @BotFather — it "
                "looks like 123456789:AAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    if code == 400 and "chat not found" in d:
        return ("Chat ID not found. Send any message to your bot first, then "
                "get your chat ID from @userinfobot.")
    if code == 403:
        return ("Your bot cannot message you yet. Open a chat with it in "
                "Telegram and press Start, then try again.")
    if code == 429:
        return "Too many requests to Telegram. Wait a moment and try again."
    return description[:200]


def send_telegram_message(token: str, chat_id: str, text: str,
                          reply_to_message_id=None,
                          disable_preview: bool = True) -> dict:
    """Send one HTML-formatted message.

    Returns {ok, message_id} on success, or {ok: False, error, message}.
    `text` must already be HTML-safe — build it with escape_html() for any
    value that came from market data or a model.
    """
    token   = (token or "").strip()
    chat_id = str(chat_id or "").strip()

    if not token or not chat_id:
        return {"ok": False, "error": "not_configured",
                "message": "Telegram bot token and chat ID are both required."}
    if not _valid_token_shape(token):
        return {"ok": False, "error": "bad_token_format",
                "message": ("That does not look like a bot token. It should be "
                            "digits, a colon, then a long code.")}

    body = (text or "").strip()
    if not body:
        return {"ok": False, "error": "empty_message",
                "message": "Nothing to send."}
    if len(body) > _MAX_LEN:
        body = body[:_MAX_LEN - 20].rstrip() + "\n…(truncated)"

    payload = {
        "chat_id":    chat_id,
        "text":       body,
        "parse_mode": "HTML",
        "link_preview_options": {"is_disabled": bool(disable_preview)},
    }
    if reply_to_message_id:
        # Keeps outcome follow-ups threaded under the original alert. If the
        # original was deleted, Telegram would reject the send — allow_sending
        # _without_reply makes it arrive as a normal message instead.
        payload["reply_parameters"] = {
            "message_id": int(reply_to_message_id),
            "allow_sending_without_reply": True,
        }

    res = _post(token, "sendMessage", payload)
    if not res.get("ok"):
        return res
    return {"ok": True, "message_id": (res.get("result") or {}).get("message_id")}


def test_telegram_connection(token: str, chat_id: str) -> dict:
    """Verify a token/chat pair by identifying the bot, then messaging the user.

    Two steps so the error can name which half is wrong: a bad token fails at
    getMe, a bad chat ID fails at sendMessage.
    """
    token   = (token or "").strip()
    chat_id = str(chat_id or "").strip()

    if not token:
        return {"ok": False, "error": "missing_token",
                "message": "Paste your bot token first."}
    if not _valid_token_shape(token):
        return {"ok": False, "error": "bad_token_format",
                "message": ("That does not look like a bot token. It should be "
                            "digits, a colon, then a long code — copy it again "
                            "from @BotFather.")}
    if not chat_id:
        return {"ok": False, "error": "missing_chat_id",
                "message": "Paste your chat ID first."}

    who = _post(token, "getMe", {})
    if not who.get("ok"):
        return who
    bot_username = (who.get("result") or {}).get("username") or "your bot"

    sent = send_telegram_message(
        token, chat_id,
        "<b>✅ Connected</b>\n"
        "Live Monitor can now send you setup alerts here.",
    )
    if not sent.get("ok"):
        return sent

    return {"ok": True,
            "bot_username": bot_username,
            "message_id":   sent.get("message_id"),
            "message":      f"Connected to @{bot_username}. Check Telegram for a test message."}
