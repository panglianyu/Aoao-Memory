"""One admission counter shared by the task loop and research requests."""
MAX_TASK_MODEL_CALLS = 8


def validate_model_call_count(
    value, *, research_calls: int = 0, terminal_summary: bool = False,
) -> int:
    upper_bound = MAX_TASK_MODEL_CALLS + (1 if terminal_summary else 0)
    if (type(value) is not int or not 0 <= value <= upper_bound
            or type(research_calls) is not int or research_calls < 0
            or value < research_calls):
        raise ValueError("invalid_model_usage")
    return value


def reserve_model_call(run) -> None:
    validate_model_call_count(run.llm_rounds)
    if run.llm_rounds >= MAX_TASK_MODEL_CALLS:
        raise ValueError("budget_exhausted")
    run.llm_rounds += 1


def prepare_model_attempt(brain, *, retry: bool, summary_only: bool = False):
    """Disable SDK retries and admit each application-level retry explicitly."""
    client = brain.client
    client.max_retries = 0
    if retry:
        run = getattr(brain, "_current_run", None)
        if run is not None:
            reserve_model_call(run)
    return client.with_options(max_retries=0) if summary_only else client
