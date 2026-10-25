{% macro transport_mode(route_type) %}
    {#- HSL's GTFS route types (docs/data_sources.md); 7xx are the bus types. -#}
    case
        when {{ route_type }} = 0 then 'tram'
        when {{ route_type }} = 1 then 'metro'
        when {{ route_type }} = 4 then 'ferry'
        when {{ route_type }} = 109 then 'train'
        when {{ route_type }} between 700 and 799 then 'bus'
        when {{ route_type }} = 900 then 'light rail'
        else 'other'
    end
{% endmacro %}
