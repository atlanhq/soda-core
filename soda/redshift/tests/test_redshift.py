import logging
from unittest.mock import patch

import pytest
from soda.common.logs import Logs
from soda.data_sources.redshift_data_source import RedshiftDataSource
from soda.execution.data_source import DataSource


def redshift_data_source(**properties) -> RedshiftDataSource:
    # username/password are set so that __init__ does not go looking for AWS cluster credentials.
    return RedshiftDataSource(
        Logs(logging.getLogger(__name__)),
        "test_redshift",
        {"username": "soda", "password": "soda", **properties},
    )


def test_redshift():
    """Add plugin specific tests here. Present so that CI is simpler and to avoid false plugin-specific tests passing."""


def test_redshift_profiles_spectrum_string_columns():
    """Redshift Spectrum / external tables report text columns as 'string' via SVV_COLUMNS.

    ProfileColumnsRun classifies a column with
    `column_data_type.startswith(tuple(TEXT_TYPES_FOR_PROFILING))`, so 'string' must be in the
    list or every text column on an external table is silently skipped.
    """
    text_types = tuple(RedshiftDataSource.TEXT_TYPES_FOR_PROFILING)

    # The exact comparison ProfileColumnsRun performs. SVV_COLUMNS returns lowercase type names.
    assert "string".startswith(text_types)

    # The native Redshift text types must keep working.
    for native_type in DataSource.TEXT_TYPES_FOR_PROFILING:
        assert native_type.startswith(text_types)


def test_redshift_applies_a_default_statement_timeout():
    """Without a server-side timeout, a query that never returns blocks the whole run forever (GOV-991).

    psycopg2 has no query timeout of its own, so a profiling run that loses a query to a stalled
    warehouse waits on the socket indefinitely and the remaining tables are never profiled.
    """
    expected_ms = RedshiftDataSource.DEFAULT_QUERY_TIMEOUT_SEC * 1000

    assert f"-c statement_timeout={expected_ms}" in redshift_data_source().connection_options()


def test_redshift_statement_timeout_is_configurable():
    assert "-c statement_timeout=900000" in redshift_data_source(query_timeout_sec=900).connection_options()


def test_redshift_statement_timeout_can_be_disabled():
    """0 means "no timeout" - the Redshift/Postgres meaning of statement_timeout=0."""
    options = redshift_data_source(schema="example_schema", query_timeout_sec=0).connection_options()

    assert options == "-c search_path=example_schema"


def test_redshift_search_path_is_kept_alongside_the_timeout():
    options = redshift_data_source(schema="example_schema").connection_options()

    assert "-c search_path=example_schema" in options
    assert "-c statement_timeout=" in options


@pytest.mark.parametrize(
    "value",
    [
        None,  # `query_timeout_sec:` present but left blank in the connection YAML
        -5,
        0.5,  # int() would truncate this to 0, i.e. to "no timeout"
        True,  # YAML reads `yes` as True, and int() would make that a 1 second timeout
        "abc",
        "",
    ],
)
def test_redshift_unusable_query_timeout_sec_falls_back_to_the_default(value):
    """An unusable value must not be a quieter route to the unbounded wait of GOV-991.

    Only an explicit 0 disables the timeout; everything else that cannot be read as whole
    seconds keeps the default, so the ceiling is never removed by a config mistake.
    """
    expected_ms = RedshiftDataSource.DEFAULT_QUERY_TIMEOUT_SEC * 1000

    options = redshift_data_source(query_timeout_sec=value).connection_options()

    assert options == f"-c statement_timeout={expected_ms}"


def test_redshift_connection_options_are_none_when_nothing_to_set():
    """psycopg2 expects None rather than an empty options string."""
    assert redshift_data_source(query_timeout_sec=0).connection_options() is None


def test_redshift_connect_passes_the_options_to_psycopg2():
    """A correct options string bounds nothing unless it reaches the driver.

    Every other test here asserts on connection_options() alone, so dropping the options kwarg
    from connect() would leave them all green while shipping no timeout at all.
    """
    data_source = redshift_data_source(query_timeout_sec=900)

    with patch("soda.data_sources.redshift_data_source.psycopg2.connect") as psycopg2_connect:
        data_source.connect()

    assert psycopg2_connect.call_args.kwargs["options"] == "-c statement_timeout=900000"
