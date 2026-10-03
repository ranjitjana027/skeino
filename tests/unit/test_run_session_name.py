import pytest

from skeino.ops.runs import _session_name_of


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"langsmith_session_name": "proj"}, "proj"),
        ({"langsmith_session_name": ""}, None),
        ({"langsmith_session_name": 123}, None),
        ({"langsmith_session_name": ["x"]}, None),
        ({}, None),
        (None, None),
    ],
)
def test_session_name_of_ignores_non_string_values(
    kwargs: object, expected: str | None
) -> None:
    assert _session_name_of(kwargs) == expected
