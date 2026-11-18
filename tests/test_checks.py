import logging
from dataclasses import asdict
from functools import partial
from pathlib import Path

import pandas as pd
import pytest

from simple_dag import BinaryOutput, JsonInput, JsonOutput, PandasDFOutput
from simple_dag.checks import CheckResult, check_id, evaluate_check


def test_boolean_and_exception_checks():
    assert evaluate_check(lambda x: True, None).status == "passed"
    assert evaluate_check(lambda x: False, None).status == "failed"
    assert evaluate_check(lambda x: 1 / 0, None).status == "error"


def test_check_passes_on_a_truthy_result_that_is_not_the_true_singleton():
    def all_positive(frame):
        return (frame["value"] > 0).all()  # a numpy.bool_

    positive = pd.DataFrame({"value": [1, 2]})
    negative = pd.DataFrame({"value": [1, -2]})
    assert evaluate_check(all_positive, positive).status == "passed"
    assert evaluate_check(all_positive, negative).status == "failed"


def test_check_without_a_return_value_fails():
    assert evaluate_check(lambda data: None, None).status == "failed"


def test_arrow_scalar_counts_by_its_value():
    class Scalar:
        """Like a scalar of older pyarrow: truthy whatever it holds."""

        def __init__(self, value):
            self.value = value

        def as_py(self):
            return self.value

    assert evaluate_check(lambda data: Scalar(True), None).status == "passed"
    assert evaluate_check(lambda data: Scalar(False), None).status == "failed"
    assert evaluate_check(lambda data: Scalar(None), None).status == "failed"


def test_arrow_compute_result_decides_a_check():
    pa = pytest.importorskip("pyarrow")
    pc = pytest.importorskip("pyarrow.compute")

    def all_positive(table):
        return pc.all(pc.greater(table["value"], 0))

    assert evaluate_check(all_positive, pa.table({"value": [1, 2]})).status == "passed"
    assert evaluate_check(all_positive, pa.table({"value": [1, -2]})).status == "failed"


def test_check_result_has_exactly_the_fields_of_protocol_1():
    assert asdict(CheckResult("has_rows", "passed")) == {
        "check_id": "has_rows",
        "status": "passed",
        "stage": "output",
        "severity": "blocking",
        "version": "1",
        "metrics": {},
        "duration_ms": 0,
        "protocol": 1,
    }


def test_raising_check_is_logged_and_reported_as_error(caplog):
    def has_orders(data):
        raise KeyError("column orders is missing")

    with caplog.at_level(logging.ERROR, logger="simple_dag"):
        result = evaluate_check(has_orders, None, "input")
    assert (result.check_id, result.status, result.stage) == (
        "has_orders",
        "error",
        "input",
    )
    assert "column orders is missing" in caplog.text


def test_check_id_and_severity_come_from_function_attributes():
    def not_empty(data):
        return False

    not_empty.check_id = "orders-not-empty"
    not_empty.severity = "warning"
    result = evaluate_check(not_empty, None)
    assert (result.check_id, result.severity, result.status) == (
        "orders-not-empty",
        "warning",
        "failed",
    )


def test_callable_without_a_name_is_reported_under_its_type():
    at_least = partial(lambda minimum, data: len(data) >= minimum, 2)
    assert check_id(at_least) == "partial"
    assert evaluate_check(at_least, [1, 2]).status == "passed"


def test_structured_result_is_passed_on_unchanged():
    custom = CheckResult("row-count", "failed", "output", "warning", metrics={"n": 0})
    assert evaluate_check(lambda data: custom, None) is custom


def test_blocking_check_stops_the_write_and_names_the_check(tmp_path):
    def has_payload(data):
        return len(data) > 0

    output = BinaryOutput(str(tmp_path / "data.bin"), health_checks=[has_payload])
    with pytest.raises(
        ValueError,
        match=r"Output checks failed for .*data\.bin: has_payload \(failed\)",
    ):
        output.write_data(b"")
    assert not Path(output.path).exists()
    assert [result.status for result in output.check_results] == ["failed"]


def test_warning_check_is_recorded_without_stopping_the_write(tmp_path):
    def never(data):
        return False

    never.severity = "warning"
    output = BinaryOutput(str(tmp_path / "data.bin"), health_checks=[never])
    output.write_data(b"kept")
    assert Path(output.path).read_bytes() == b"kept"
    assert [result.status for result in output.check_results] == ["failed"]


def test_every_check_runs_and_every_failure_is_named(tmp_path):
    def has_rows(frame):
        return len(frame) > 0

    def has_total(frame):
        raise KeyError("total")

    def has_columns(frame):
        return len(frame.columns) > 0

    output = PandasDFOutput(
        str(tmp_path / "table.csv"), health_checks=[has_rows, has_total, has_columns]
    )
    with pytest.raises(ValueError, match=r"has_rows \(failed\), has_total \(error\)$"):
        output.write_data(pd.DataFrame({"value": []}))
    assert [result.status for result in output.check_results] == [
        "failed",
        "error",
        "passed",
    ]


def test_input_checks_see_what_the_transform_receives(tmp_path):
    path = str(tmp_path / "numbers.json")
    JsonOutput(path).write_data([1, 2, 3])
    numbers = JsonInput(path, health_checks=[lambda data: sum(data) == 6])
    assert numbers.get_data() == [1, 2, 3]
    assert [(result.stage, result.status) for result in numbers.check_results] == [
        ("input", "passed")
    ]
