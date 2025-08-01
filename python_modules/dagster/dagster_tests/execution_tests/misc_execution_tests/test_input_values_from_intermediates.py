import dagster as dg


def test_from_intermediates_from_multiple_outputs() -> None:
    @dg.op
    def x():
        return "x"

    @dg.op
    def y():
        return "y"

    @dg.op(ins={"stuff": dg.In(dg.Optional[dg.List[str]])})
    def gather(stuff):
        return "{} and {}".format(*stuff)

    @dg.job
    def pipe():
        gather([x(), y()])

    result = pipe.execute_in_process()

    assert result
    assert result.success
    step_input_data = next(
        evt.step_input_data
        for evt in result.events_for_node("gather")
        if evt.event_type_value == "STEP_INPUT"
    )
    assert step_input_data.input_name == "stuff"
    assert step_input_data.type_check_data.label == "stuff"
    assert result.output_for_node("gather") == "x and y"


def test_from_intermediates_from_config() -> None:
    run_config = {"ops": {"x": {"inputs": {"string_input": {"value": "Dagster is great!"}}}}}

    @dg.op
    def x(string_input):
        return string_input

    @dg.job
    def pipe():
        x()

    result = pipe.execute_in_process(run_config=run_config)

    assert result
    assert result.success
    step_input_data = next(
        evt.step_input_data
        for evt in result.events_for_node("x")
        if evt.event_type_value == "STEP_INPUT"
    )
    assert step_input_data.input_name == "string_input"
    assert step_input_data.type_check_data.label == "string_input"
    assert result.output_for_node("x") == "Dagster is great!"
