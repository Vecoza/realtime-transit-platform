{% macro is_suspect_sign_on(tst, service_date, last_arrival_offset_seconds) %}
    {#- The aggregator's rule (processor/windows.py): an event more than
        SUSPECT_SIGN_ON_AFTER_S after its trip's planned end belongs to a vehicle still
        signed on to a journey it is no longer running. -#}
    ({{ tst }} > {{ service_day_start(service_date) }}
        + to_seconds({{ last_arrival_offset_seconds }})
        + to_seconds(cast({{ var('suspect_sign_on_after_s') }} as bigint)))
{% endmacro %}
