{% test unique_combination(model, columns) %}
    select {{ columns | join(', ') }}, count(*) as occurrences
    from {{ model }}
    group by {{ columns | join(', ') }}
    having count(*) > 1
{% endtest %}


{% test between(model, column_name, min_value, max_value) %}
    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }}
{% endtest %}
