{% macro generate_schema_name(custom_schema_name, node) -%}
  {{ custom_schema_name | trim if custom_schema_name else target.schema }}
{%- endmacro %}

{% macro text_field(field, payload='payload_json') -%}
nullif(trim(json_extract_string({{ payload }}, '$.{{ field }}')), '')
{%- endmacro %}

{% macro integer_field(field, payload='payload_json') -%}
case when regexp_full_match({{ text_field(field, payload) }}, '[+-]?[0-9]+([.]0+)?')
then try_cast({{ text_field(field, payload) }} as bigint) end
{%- endmacro %}

{% macro number_field(field, payload='payload_json') -%}
{%- set value = text_field(field, payload) -%}
{%- set digits = "regexp_replace(split_part(lower(" ~ value ~ "), 'e', 1), '[.+-]', '', 'g')" -%}
-- Reject nonzero precision beyond the declared scale instead of rounding before hashing.
case when regexp_full_match({{ value }}, '[+-]?[0-9]+([.][0-9]+)?([eE][+-]?[0-9]{1,4})?')
    and (regexp_full_match({{ digits }}, '0+') or
        length(regexp_extract({{ value }}, '[.]([0-9]+)', 1))
        - (length({{ digits }}) - length(rtrim({{ digits }}, '0')))
        - coalesce(try_cast(regexp_extract({{ value }}, '[eE]([+-]?[0-9]+)$', 1) as integer), 0) <= 6)
then try_cast({{ value }} as decimal(18,6)) end
{%- endmacro %}

{% macro utc_timestamp(expression) -%}
case when regexp_matches({{ expression }}, '(Z|[+-][0-9]{2}:?[0-9]{2})$', 'i')
then try_cast({{ expression }} as timestamptz)
else timezone('{{ var("study_timezone") }}', try_cast({{ expression }} as timestamp)) end
{%- endmacro %}
