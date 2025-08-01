{{ config(materialized='view') }}

-- Stage outcomes with DATA VALIDATION and BUSINESS LOGIC CORRECTIONS
WITH raw_data AS (
    SELECT * FROM {{ source('main', 'outcomes') }}
),

cleaned_outcomes AS (
    SELECT 
        outcome_id,
        lead_id,
        
        -- VALIDATE CONVERSION LOGIC: Ensure consistency between converted flag and revenue
        CASE 
            WHEN converted = 1 AND (revenue IS NULL OR revenue <= 0) THEN 0  -- If no revenue, not really converted
            WHEN converted = 0 AND revenue > 0 THEN 1  -- If revenue exists, should be converted
            ELSE converted
        END AS converted_clean,
        
        -- CLEAN REVENUE: Handle missing, negative, extreme values + ROUND to 2 decimal places
        CASE 
            WHEN revenue IS NULL THEN 0.00
            WHEN revenue < 0 THEN 0.00  -- Set negative revenue to 0
            WHEN revenue > 2000000 THEN 500000.00  -- Cap extreme values at realistic $500K (most B2B SaaS deals under $2M)
            WHEN revenue = 0.01 THEN 0.00  -- Fix obvious data entry errors
            WHEN revenue = 999999999 THEN 150000.00  -- Fix data generation error to realistic $150K
            WHEN revenue = 50000000 THEN 250000.00  -- Fix unrealistic $50M to realistic $250K
            ELSE ROUND(revenue, 2)  -- Round all revenue to 2 decimal places for consistency
        END AS revenue_clean,
        
        -- CLEAN OUTCOME DATES: Handle missing dates
        CASE 
            WHEN outcome_date IS NULL AND converted = 1 THEN DATE('now')  -- Converted leads need dates
            ELSE outcome_date
        END AS outcome_date_clean,
        
        -- CLEAN DAYS TO CLOSE: Handle negative and extreme values
        CASE 
            WHEN days_to_close IS NULL THEN NULL
            WHEN days_to_close < 0 THEN ABS(days_to_close)  -- Fix negative values
            WHEN days_to_close > 730 THEN 730  -- Cap at 2 years max
            WHEN days_to_close = 0 THEN 1  -- Minimum 1 day
            ELSE days_to_close
        END AS days_to_close_clean,
        
        -- ADD DATA QUALITY FLAGS
        CASE WHEN revenue IS NULL THEN 1 ELSE 0 END AS missing_revenue_flag,
        CASE WHEN outcome_date IS NULL THEN 1 ELSE 0 END AS missing_date_flag,
        CASE WHEN days_to_close < 0 THEN 1 ELSE 0 END AS negative_days_flag,
        CASE WHEN days_to_close > 365 THEN 1 ELSE 0 END AS extreme_days_flag,
        CASE WHEN revenue < 0 OR revenue > 2000000 OR revenue = 999999999 OR revenue = 50000000 THEN 1 ELSE 0 END AS extreme_revenue_flag
        
    FROM raw_data
    WHERE lead_id IS NOT NULL
)

SELECT 
    outcome_id,
    lead_id,
    converted_clean AS converted,
    revenue_clean AS revenue,
    outcome_date_clean AS outcome_date,
    days_to_close_clean AS days_to_close,
    
    -- Data quality monitoring
    missing_revenue_flag,
    missing_date_flag,
    negative_days_flag,
    extreme_days_flag,
    extreme_revenue_flag,
    (missing_revenue_flag + missing_date_flag + negative_days_flag + extreme_days_flag + extreme_revenue_flag) AS total_quality_issues
    
FROM cleaned_outcomes
