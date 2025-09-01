{% macro pii_tag_check() %}
{% if execute %}
  {% set current_model = model %}
  {% set model_tags = current_model.tags or [] %}   
  {% set pii_schemas = ['dbt_pcoomber_staging_pii', 'intermediate_pii'] %}
  {% set required_tags = ['PII'] %}

  {{ log("Current model schema: " ~ current_model.schema, info=true) }}
  {% if current_model.schema in pii_schemas %}
   {% for required_tag in required_tags %}
    {% if required_tag not in model_tags %}
           {{ exceptions.raise_compiler_error(
           "Model " ~ current_model.name ~ " is in PII schema but missing required tag: " ~ 
           required_tag
           ) }}
    {% endif %}
   {% endfor %}        
  {% endif %}

  {% for parent in current_model.depends_on.nodes %}
    {% if parent.startswith('model.') %}
      {% set upstream_model = graph.nodes[parent] %}
      {% if upstream_model.schema in pii_schemas %}
        {% for required_tag in required_tags %}
          {% if required_tag not in model_tags %}
            {{ exceptions.raise_compiler_error(
              "Model " ~ current_model.name ~ " uses upstream model " ~ upstream_model.name ~ 
              " from PII schema but missing required tag: " ~ 
              required_tag
            ) }}
          {% endif %}
        {% endfor %}        
      {% endif %}
    {% endif %}
  {% endfor %}   
{% endif %}
{% endmacro %}