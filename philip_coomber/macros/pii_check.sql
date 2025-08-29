{% macro pii_tag_check() %}
 {% if execute %}
   {% set current_model = model %}
   {% set model_tags = current_model.tags or [] %}   
   {% set pii_schemas = ['staging_pii', 'intermediate_pii'] %}
   {% set required_tags = ['PII'] %}
   
   {% set upstream_models = [] %}
   {% for parent in current_model.depends_on.nodes %}
     {% if parent.startswith('model.') %}
       {% if upstream_model.schema in pii_schemas %}
         {% for required_tag in required_tags %}
           {% if required_tag not in model_tags %}
             -- CAN RAISE AN EXCEPTION OR MITIGATE FOR THE USER
             {{ exceptions.raise_compiler_error(
               "Model " ~ current_model.name ~ " uses upstream models from restricted schemas " ~ 
               restricted_upstream_list | join(", ") ~ " but is missing required tags: " ~ 
               missing_tags | join(", ")
             ) }}
           {% endif %}
         {% endfor %}        
       {% endif %}
     {% endif %}
   {% endfor %}   
 {% endif %}
{% endmacro %}