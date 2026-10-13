{#- Generic test: fails on every row where the column is negative.
    NULL passes, as in dbt's built-in tests: combine with not_null when NULL is not allowed. -#}
{% test non_negative(model, column_name) %}

select {{ column_name }} as negative_value
from {{ model }}
where {{ column_name }} < 0

{% endtest %}
