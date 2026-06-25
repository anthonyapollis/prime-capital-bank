{% macro assign_risk_segment(credit_score_col, delinquency_days_col, fraud_count_col, aml_flag_col) %}
    /*
    Assigns a customer risk segment based on a composite of:
    - Credit score (TransUnion/Experian 300-850 scale)
    - Delinquency days (DPD on any active loan)
    - Historical fraud events
    - AML flag (active AML case)

    Segments: Prime / Standard / Substandard / Doubtful / Loss
    Maps to SARB risk classifications for provisioning purposes.
    */
    case
        when {{ aml_flag_col }} = true
            then 'Loss'
        when {{ delinquency_days_col }} > 90 or {{ fraud_count_col }} >= 3
            then 'Loss'
        when {{ delinquency_days_col }} between 61 and 90 or {{ fraud_count_col }} = 2
            then 'Doubtful'
        when {{ delinquency_days_col }} between 31 and 60
            then 'Substandard'
        when {{ delinquency_days_col }} between 1 and 30
            or {{ credit_score_col }} < 550
            or {{ fraud_count_col }} = 1
            then 'Standard'
        when {{ credit_score_col }} >= 700 and {{ delinquency_days_col }} = 0
            then 'Prime'
        else 'Standard'
    end
{% endmacro %}


{% macro provision_rate(risk_segment_col) %}
    /*
    Maps risk segment to IFRS 9 provision rate.
    Prime / Standard = Stage 1 (12-month ECL)
    Substandard      = Stage 2 (lifetime ECL, no default)
    Doubtful / Loss  = Stage 3 (lifetime ECL, credit-impaired)
    */
    case {{ risk_segment_col }}
        when 'Prime'        then 0.005   -- 0.5% Stage 1
        when 'Standard'     then 0.01    -- 1.0% Stage 1
        when 'Substandard'  then 0.25    -- 25%  Stage 2
        when 'Doubtful'     then 0.50    -- 50%  Stage 3
        when 'Loss'         then 1.00    -- 100% Stage 3 (full provision)
        else 0.01
    end
{% endmacro %}


{% macro income_band(annual_income_col) %}
    /*
    Segments customers by annual income for product targeting.
    Based on South African income distribution (Stats SA).
    */
    case
        when {{ annual_income_col }} < 60000          then 'R0-60K (Low)'
        when {{ annual_income_col }} < 180000         then 'R60K-180K (Lower Middle)'
        when {{ annual_income_col }} < 360000         then 'R180K-360K (Middle)'
        when {{ annual_income_col }} < 750000         then 'R360K-750K (Upper Middle)'
        when {{ annual_income_col }} < 1500000        then 'R750K-1.5M (Affluent)'
        else                                               'R1.5M+ (High Net Worth)'
    end
{% endmacro %}
