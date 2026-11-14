{% test positive(model, column_name) %}
-- Prices must be greater than zero when present.
select {{ column_name }} from {{ model }}
where {{ column_name }} is not null and {{ column_name }} <= 0
{% endtest %}
