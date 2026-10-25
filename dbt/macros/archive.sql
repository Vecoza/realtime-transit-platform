{% macro archived_days(table) %}
    {#- UTC days whose partition was dropped after a verified export. -#}
    select distinct cast(object_key as date) as utc_day
    from pg.meta.archive_runs
    where source_table = 'rt.{{ table }}' and step = 'dropped'
{% endmacro %}


{% macro archive_glob(path) %}
    {{ return(var('archive_dir') ~ '/' ~ path ~ '/*/*.parquet') }}
{% endmacro %}


{% macro has_archive_files(path) %}
    {#- read_parquet fails on an empty glob, so a source without files is left out. -#}
    {% if not execute %}
        {{ return(false) }}
    {% endif %}
    {% set found = run_query("select count(*) from glob('" ~ archive_glob(path) ~ "')") %}
    {{ return(found.columns[0].values()[0] > 0) }}
{% endmacro %}


{% macro read_archive(path) %}
    read_parquet('{{ archive_glob(path) }}', hive_partitioning = true, union_by_name = true)
{% endmacro %}
