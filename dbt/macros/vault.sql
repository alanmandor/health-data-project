{% macro dv_hash(entity, fields) -%}
sha256(to_json(list_value('{{ entity }}'{% for field in fields %}, cast({{ field }} as varchar){% endfor %})))
{%- endmacro %}
