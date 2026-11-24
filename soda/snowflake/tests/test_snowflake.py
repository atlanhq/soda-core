import logging
from unittest import mock

from soda.common.logs import Logs
from soda.data_sources.snowflake_data_source import SnowflakeDataSource


def test_snowflake():
    """Add plugin specific tests here. Present so that CI is simpler and to avoid false plugin-specific tests passing."""


def test_connect_keeps_ocsp_revocation_checks_on():
    # snowflake-connector-python 4.8 turned OCSP revocation checks off unless
    # ocsp_fail_open is passed; 3.x ran them fail-open by default.
    data_source = SnowflakeDataSource(
        Logs(logging.getLogger("soda.scan")), "snowflake", {"account": "a", "username": "u", "password": "p"}
    )
    with mock.patch("soda.data_sources.snowflake_data_source.connector.connect") as connect:
        data_source.connect()
    assert connect.call_args.kwargs["ocsp_fail_open"] is True
