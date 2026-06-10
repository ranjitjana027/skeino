"""Per-run token-usage measurement for the ``end`` event and usage header.

The streaming serializer (:mod:`skeino.serialization.serialize`) deliberately
strips ``usage_metadata`` / ``response_metadata`` to keep the wire format lean,
so token counts are not visible to downstream consumers (e.g. the gateway that
enforces rate limits). Usage is therefore measured server-side and surfaced
explicitly in the ``end`` SSE event and the ``X-Tokens-Used`` response header.

Primary mechanism: a :class:`UsageMetadataCallbackHandler` attached to each
run's config (:func:`attach_usage_handler`), which records every LLM call made
during the run — including calls whose responses never reach checkpoint state —
and is inherently scoped to that run. Fallback: summing ``usage_metadata`` /
``response_metadata`` over the final checkpoint's messages
(:func:`total_tokens_from_messages`), for providers that don't populate the
fields the callback handler requires (``usage_metadata`` + ``model_name``).
"""

from collections.abc import Mapping
from typing import Any

from langchain_core.callbacks import UsageMetadataCallbackHandler


def attach_usage_handler(config: dict[str, Any]) -> UsageMetadataCallbackHandler:
    """Attach a fresh usage handler to a run config and return it.

    Appends to any caller-supplied ``callbacks`` list rather than clobbering
    it. LangChain propagates config callbacks to every nested LLM call inside
    graph nodes (the same contextvar mechanism tracing uses), so the handler
    sees the whole run.
    """
    handler = UsageMetadataCallbackHandler()
    existing = config.get("callbacks")
    config["callbacks"] = (
        [*existing, handler] if isinstance(existing, list) else [handler]
    )
    return handler


def total_tokens_from_usage(usage_by_model: Mapping[str, Any]) -> int:
    """Sum ``total_tokens`` across a usage handler's per-model aggregates."""
    total = 0
    for usage in usage_by_model.values():
        if isinstance(usage, Mapping):
            total += int(usage.get("total_tokens", 0))
    return total


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
