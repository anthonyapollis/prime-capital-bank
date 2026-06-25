{% macro convert_to_zar(amount_col, currency_col, rate_col=none) %}
    /*
    Converts any currency amount to ZAR.
    If rate_col is provided, uses that column directly.
    Otherwise falls back to the dim_currency table lookup.
    */
    case
        when {{ currency_col }} = 'ZAR' or {{ currency_col }} is null
            then {{ amount_col }}
        {% if rate_col %}
        when {{ rate_col }} is not null and {{ rate_col }} > 0
            then {{ amount_col }} * {{ rate_col }}
        {% endif %}
        else {{ amount_col }} * coalesce(
            (
                select to_zar_rate
                from {{ ref('dim_currency') }}
                where currency_code = {{ currency_col }}
                  and rate_date = current_date()
                limit 1
            ),
            1.0  -- fallback: treat as ZAR if no rate found
        )
    end
{% endmacro %}


{% macro zar_exchange_rates() %}
    /*
    Returns a CTE of current exchange rates to ZAR.
    Usage: {{ zar_exchange_rates() }} then JOIN on currency_code.
    */
    select
        currency_code,
        currency_name,
        to_zar_rate,
        rate_date
    from {{ ref('dim_currency') }}
    where rate_date = (select max(rate_date) from {{ ref('dim_currency') }})
{% endmacro %}
