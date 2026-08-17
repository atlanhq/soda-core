import logging
import re
from typing import List, Optional

import boto3
import psycopg2
from soda.common.aws_credentials import AwsCredentials
from soda.common.logs import Logs
from soda.execution.data_source import DataSource

logger = logging.getLogger(__name__)


class RedshiftDataSource(DataSource):
    TYPE = "redshift"

    # Redshift Spectrum / external tables surface Glue-Hive type names through SVV_COLUMNS, where text
    # columns report as 'string' instead of a native Redshift type name. Without 'string' here, every text
    # column on an external table is skipped by profiling with "not in supported profiling data types".
    TEXT_TYPES_FOR_PROFILING = DataSource.TEXT_TYPES_FOR_PROFILING + ["string"]

    # psycopg2 has no query timeout of its own, so a query the warehouse never answers leaves the
    # scan blocked on the socket forever and the rest of the run is never profiled. This is a ceiling
    # on pathological queries, not a performance target - profiling queries normally take seconds.
    # Override with query_timeout_sec in the data source connection properties, or 0 to disable. On a
    # profiling playbook that arrives through the "Runtime Connection Properties" field, which is
    # merged over the stored connection, e.g. {"query_timeout_sec": 900}.
    DEFAULT_QUERY_TIMEOUT_SEC = 3600

    def __init__(self, logs: Logs, data_source_name: str, data_source_properties: dict):
        super().__init__(logs, data_source_name, data_source_properties)

        self.host = data_source_properties.get("host", "localhost")
        self.port = data_source_properties.get("port", "5439")
        self.connect_timeout = data_source_properties.get("connection_timeout_sec")
        self.query_timeout_sec = self.__resolve_query_timeout_sec(data_source_properties.get("query_timeout_sec"))
        self.username = data_source_properties.get("username")
        self.password = data_source_properties.get("password")
        self.dbuser = data_source_properties.get("dbuser")
        self.dbname = data_source_properties.get("dbname")
        self.cluster_id = data_source_properties.get("cluster_id")

        if not self.username or not self.password:
            aws_credentials = AwsCredentials(
                access_key_id=data_source_properties.get("access_key_id"),
                secret_access_key=data_source_properties.get("secret_access_key"),
                role_arn=data_source_properties.get("role_arn"),
                session_token=data_source_properties.get("session_token"),
                region_name=data_source_properties.get("region", "eu-west-1"),
                profile_name=data_source_properties.get("profile_name"),
                external_id=data_source_properties.get("external_id"),
            )
            self.username, self.password = self.__get_cluster_credentials(aws_credentials)

    def __resolve_query_timeout_sec(self, value) -> int:
        """Resolve query_timeout_sec to whole seconds, where only an explicit 0 disables the timeout.

        Anything unusable falls back to the default rather than to no timeout at all: silently
        dropping the ceiling is the failure this exists to prevent, so a bad value must not be a
        quieter way of reaching it. Booleans are rejected because YAML reads `yes` as True, which
        int() turns into a 1 second timeout that empties every profile.
        """
        if value is None:
            return self.DEFAULT_QUERY_TIMEOUT_SEC

        timeout_sec = None
        if isinstance(value, int) and not isinstance(value, bool):
            timeout_sec = value
        elif isinstance(value, str):
            try:
                timeout_sec = int(value.strip())
            except ValueError:
                pass

        if timeout_sec is None or timeout_sec < 0:
            self.logs.error(
                f"Invalid query_timeout_sec {value!r} on data source '{self.data_source_name}': expected whole "
                f"seconds >= 0, where 0 disables the timeout. Using the default of "
                f"{self.DEFAULT_QUERY_TIMEOUT_SEC}s."
            )
            return self.DEFAULT_QUERY_TIMEOUT_SEC

        return timeout_sec

    def connection_options(self) -> Optional[str]:
        options = []
        if self.schema:
            options.append(f"-c search_path={self.schema}")

        if self.query_timeout_sec > 0:
            options.append(f"-c statement_timeout={self.query_timeout_sec * 1000}")

        return " ".join(options) if options else None

    def connect(self):
        options = self.connection_options()

        # Logged so that a run's own logs answer whether the timeout was in force, and at what value.
        self.logs.info(f"Redshift query timeout: {self.query_timeout_sec}s (0 disables), options: {options}")

        self.connection = psycopg2.connect(
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            connect_timeout=self.connect_timeout,
            database=self.database,
            options=options,
        )

    def __get_cluster_credentials(self, aws_credentials: AwsCredentials):
        resolved_aws_credentials = aws_credentials.resolve_role(
            role_session_name="soda_redshift_get_cluster_credentials"
        )

        client = boto3.client(
            "redshift",
            region_name=resolved_aws_credentials.region_name,
            aws_access_key_id=resolved_aws_credentials.access_key_id,
            aws_secret_access_key=resolved_aws_credentials.secret_access_key,
            aws_session_token=resolved_aws_credentials.session_token,
        )

        cluster_name = self.cluster_id if self.cluster_id else self.host.split(".")[0]
        username = self.dbuser if self.dbuser else self.username
        db_name = self.dbname if self.dbname else self.database
        cluster_creds = client.get_cluster_credentials(
            DbUser=username, DbName=db_name, ClusterIdentifier=cluster_name, AutoCreate=False, DurationSeconds=3600
        )

        return cluster_creds["DbUser"], cluster_creds["DbPassword"]

    def sql_get_table_names_with_count(
        self, include_tables: Optional[List[str]] = None, exclude_tables: Optional[List[str]] = None
    ) -> str:
        table_filter_expression = self.sql_table_include_exclude_filter(
            '"table"', "schema", include_tables, exclude_tables
        )
        where_clause = f"\nWHERE {table_filter_expression} \n" if table_filter_expression else ""
        return f'SELECT "table", tbl_rows \n FROM svv_table_info {where_clause}'

    def expr_regexp_like(self, expr: str, regex_pattern: str):
        return f"{expr} ~ '{regex_pattern}'"

    def escape_regex(self, value: str):
        return re.sub(r"(\\.)", r"\\\1", value)

    def get_metric_sql_aggregation_expression(self, metric_name: str, metric_args: Optional[List[object]], expr: str):
        # TODO add all of these specific statistical aggregate functions: https://docs.aws.amazon.com/redshift/latest/dg/c_Aggregate_Functions.html
        if metric_name in [
            "stddev",
            "stddev_pop",
            "stddev_samp",
            "variance",
            "var_pop",
            "var_samp",
        ]:
            return f"{metric_name.upper()}({expr})"

        return super().get_metric_sql_aggregation_expression(metric_name, metric_args, expr)

    def expr_avg(self, expr):
        return f"AVG({expr}::real)"

    def regex_replace_flags(self) -> str:
        return ""

    def default_casify_table_name(self, identifier: str) -> str:
        return identifier.lower()

    def default_casify_column_name(self, identifier: str) -> str:
        return identifier.lower()

    def default_casify_type_name(self, identifier: str) -> str:
        return identifier.lower()

    def safe_connection_data(self):
        return [
            self.type,
            self.host,
            self.port,
            self.database,
        ]

    def sql_information_schema_columns(self) -> str:
        return "SVV_COLUMNS"
