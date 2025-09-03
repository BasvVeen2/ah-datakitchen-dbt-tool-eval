{% macro partitions_to_replace_eiei() %}

    {% set is_lastday_of_month = run_query("SELECT IF(LAST_DAY(current_date()) = current_date(), 1, 0)") %}
    {% if execute %}

        {{ print(is_lastday_of_month[0].values()) }}

        {% if is_lastday_of_month.columns[0].values()|int == 0 %}

            {{ return(['current_date',
                        'date_sub(current_date, interval 1 day)']) }}

        {% else %}

            {% set where_sql = [] %}
            {% for i in range(1,36) %}

                {{ where_sql.append('date_sub(current_date, interval '+i|string+' day)') }}

            {% endfor %}
            {{ return(where_sql) }}

        {% endif %}
    {% endif %}


{% endmacro %}