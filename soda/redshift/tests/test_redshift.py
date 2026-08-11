from soda.data_sources.redshift_data_source import RedshiftDataSource
from soda.execution.data_source import DataSource


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
