"""Unit tests for provider-agnostic token usage extraction."""

from types import SimpleNamespace

from skeino.usage import total_tokens_from_messages


def _msg(*, usage_metadata=None, response_metadata=None):
    """Build a minimal message-like object with the given metadata."""
    return SimpleNamespace(
        usage_metadata=usage_metadata,
        response_metadata=response_metadata,
    )


def test_langchain_standard_usage_metadata():
    msg = _msg(usage_metadata={"total_tokens": 1620})
    assert total_tokens_from_messages([msg]) == 1620


def test_gemini_response_metadata():
    msg = _msg(
        response_metadata={"usage_metadata": {"total_token_count": 1234}},
    )
    assert total_tokens_from_messages([msg]) == 1234


def test_openai_groq_token_usage():
    msg = _msg(response_metadata={"token_usage": {"total_tokens": 555}})
    assert total_tokens_from_messages([msg]) == 555


def test_bedrock_usage_variant():
    msg = _msg(response_metadata={"usage": {"total_tokens": 777}})
    assert total_tokens_from_messages([msg]) == 777


def test_prefers_message_usage_metadata_over_response_metadata():
    # usage_metadata on the message wins over response_metadata.
    msg = _msg(
        usage_metadata={"total_tokens": 100},
        response_metadata={"token_usage": {"total_tokens": 999}},
    )
    assert total_tokens_from_messages([msg]) == 100


def test_sums_across_multiple_messages():
    msgs = [
        _msg(usage_metadata={"total_tokens": 10}),
        _msg(response_metadata={"usage_metadata": {"total_token_count": 20}}),
        _msg(response_metadata={"token_usage": {"total_tokens": 30}}),
    ]
    assert total_tokens_from_messages(msgs) == 60


def test_messages_without_usage_contribute_zero():
    msgs = [
        _msg(),
        _msg(response_metadata={}),
        _msg(usage_metadata={}),
        SimpleNamespace(),  # no metadata attributes at all
    ]
    assert total_tokens_from_messages(msgs) == 0


def test_empty_list():
    assert total_tokens_from_messages([]) == 0
