"""Provider-agnostic token-usage extraction from LangChain message lists.

The streaming serializer (:mod:`skeino.serialization.serialize`) deliberately
strips ``usage_metadata`` / ``response_metadata`` to keep the wire format lean,
so token counts are not visible to downstream consumers (e.g. the gateway that
enforces rate limits). This module recomputes the total token count from the
graph's final message list so it can be surfaced explicitly in the ``end`` SSE
event and the ``X-Tokens-Used`` response header.
"""

from typing import Any


def total_tokens_from_messages(messages: list[Any]) -> int:
    """Sum total tokens across all AI messages, normalising provider formats.

    For each AI message, tries (in order):
    1. ``msg.usage_metadata["total_tokens"]``  — LangChain standard (v0.2+)
    2. ``response_metadata["usage_metadata"]["total_token_count"]`` — Gemini
    3. ``response_metadata["token_usage"]["total_tokens"]`` — OpenAI / Groq / Bedrock
    4. ``response_metadata["usage"]["total_tokens"]`` — some Bedrock variants

    Non-AI messages and messages without recognisable usage data contribute 0.
    """
    return sum(_tokens_from_message(msg) for msg in messages)


def _tokens_from_message(msg: Any) -> int:
    # 1. LangChain standard usage_metadata on the message object.
    um = getattr(msg, "usage_metadata", None)
    if isinstance(um, dict) and um:
        v = um.get("total_tokens")
        if v is not None:
            return int(v)

    rm = getattr(msg, "response_metadata", None)
    if not isinstance(rm, dict):
        return 0

    # 2. Gemini: response_metadata["usage_metadata"]["total_token_count"]
    gemini_um = rm.get("usage_metadata")
    if isinstance(gemini_um, dict) and gemini_um:
        v = gemini_um.get("total_token_count")
        if v is not None:
            return int(v)

    # 3. OpenAI / Groq / Bedrock: response_metadata["token_usage"]["total_tokens"]
    token_usage = rm.get("token_usage")
    if isinstance(token_usage, dict) and token_usage:
        v = token_usage.get("total_tokens")
        if v is not None:
            return int(v)

    # 4. Some Bedrock variants: response_metadata["usage"]["total_tokens"]
    usage = rm.get("usage")
    if isinstance(usage, dict) and usage:
        v = usage.get("total_tokens")
        if v is not None:
            return int(v)

    return 0
