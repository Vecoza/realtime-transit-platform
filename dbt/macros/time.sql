{% macro service_day_start(service_date) %}
    {#- GTFS times count from noon minus twelve hours, local time, on the service day. -#}
    (timezone('Europe/Helsinki', cast({{ service_date }} as timestamp) + interval 12 hour)
        - interval 12 hour)
{% endmacro %}


{% macro local_hour(moment) %}
    hour(timezone('Europe/Helsinki', {{ moment }}))
{% endmacro %}
