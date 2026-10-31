"""Original competition questions survive the shared query boundaries."""

import pytest

from kernel.daily_memory_contracts import memory_query_terms
from kernel.raw_message_models import Scope
from kernel.raw_message_store import validate_raw_search
from system_memory_god.storage.daily_vector_worker import _validate_request
from system_memory_god.storage.derived_evidence import validate_evidence_search


def test_long_original_question_reaches_the_late_search_term():
    query = "context " * 100 + "needle"
    assert memory_query_terms(query) == ("context", "needle")
    assert validate_raw_search(
        Scope("agent", "owner", "connector", "conversation"), query,
        limit=20, start_ts=None, end_ts=None, before=None, deadline=None,
    ) == query
    validate_evidence_search(query, 20, None, None)
    request = {
        "version": 1, "id": 1, "scope": ["owner", "agent", "connector", "conversation"],
        "op": "search_cosine_hits", "query": query, "limit": 100,
    }
    assert _validate_request(request)["query"] == query


@pytest.mark.parametrize("length", [32768, 32769])
def test_original_question_capacity_has_an_explicit_boundary(length):
    query = "context " + "x" * (length - 8)
    if length == 32768:
        assert memory_query_terms(query) == ("context", "x" * (length - 8))
    else:
        with pytest.raises(ValueError):
            memory_query_terms(query)
