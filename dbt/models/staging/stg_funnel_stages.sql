{{ config(materialized='view') }}

-- Stage funnel stages with DATA VALIDATION and BUSINESS LOGIC
WITH raw_data AS (
    SELECT * FROM {{ source('main', 'funnel_stages') }}
),

cleaned_funnel AS (
    SELECT 
        stage_id,
        lead_id,
        
        -- STANDARDIZE STAGE NAMES: Fix inconsistent casing and validate business stages
        CASE 
            WHEN LOWER(TRIM(stage_name)) IN ('lead', 'new lead', 'initial', 'new') THEN 'New'
            WHEN LOWER(TRIM(stage_name)) IN ('contacted') THEN 'Contacted'
            WHEN LOWER(TRIM(stage_name)) IN ('qualified', 'mql', 'marketing qualified') THEN 'Qualified'
            WHEN LOWER(TRIM(stage_name)) IN ('demo scheduled', 'demo') THEN 'Demo Scheduled'
            WHEN LOWER(TRIM(stage_name)) IN ('proposal', 'quote', 'proposal sent', 'quote sent') THEN 'Proposal Sent'
            WHEN LOWER(TRIM(stage_name)) IN ('closed won', 'won', 'closed-won') THEN 'Closed Won'
            WHEN LOWER(TRIM(stage_name)) IN ('closed lost', 'lost', 'closed-lost') THEN 'Closed Lost'
            ELSE 'Unknown'
        END AS stage_name_clean,
        
        -- CLEAN STAGE DATES: Handle missing and future dates
        CASE 
            WHEN stage_date IS NULL THEN NULL
            WHEN stage_date > DATE('now') THEN DATE('now')  -- Cap future dates to today
            ELSE stage_date
        END AS stage_date_clean,
        
        -- VALIDATE STAGE ORDER: Ensure logical progression (1-7)
        CASE 
            WHEN stage_order IS NULL THEN NULL
            WHEN stage_order < 1 THEN 1
            WHEN stage_order > 7 THEN 7
            ELSE stage_order
        END AS stage_order_clean,
        
        -- BUSINESS LOGIC: Derive stage progression metrics
        ROW_NUMBER() OVER (
            PARTITION BY lead_id 
            ORDER BY stage_date ASC, stage_order ASC
        ) AS stage_sequence,
        
        LAG(stage_date) OVER (
            PARTITION BY lead_id 
            ORDER BY stage_date ASC, stage_order ASC
        ) AS previous_stage_date,
        
        -- ADD DATA QUALITY FLAGS
        CASE WHEN stage_date IS NULL THEN 1 ELSE 0 END AS missing_date_flag,
        CASE WHEN stage_date > DATE('now') THEN 1 ELSE 0 END AS future_date_flag,
        CASE WHEN stage_order IS NULL THEN 1 ELSE 0 END AS missing_order_flag,
        CASE WHEN stage_order < 1 OR stage_order > 7 THEN 1 ELSE 0 END AS invalid_order_flag
        
    FROM raw_data
    WHERE lead_id IS NOT NULL
)

SELECT 
    stage_id,
    lead_id,
    stage_name_clean AS stage_name,
    stage_date_clean AS stage_date,
    stage_order_clean AS stage_order,
    stage_sequence,
    
    -- CALCULATE STAGE DURATION: Days between stage transitions
    CASE 
        WHEN previous_stage_date IS NOT NULL 
        THEN CAST((JULIANDAY(stage_date_clean) - JULIANDAY(previous_stage_date)) AS INTEGER)
        ELSE NULL
    END AS days_in_previous_stage,
    
    -- STAGE CLASSIFICATION: Identify stage types
    CASE 
        WHEN stage_name_clean IN ('New', 'Contacted', 'Qualified') THEN 'Early Stage'
        WHEN stage_name_clean IN ('Demo Scheduled', 'Proposal Sent') THEN 'Mid Stage'
        WHEN stage_name_clean IN ('Closed Won', 'Closed Lost') THEN 'Late Stage'
        ELSE 'Unknown'
    END AS stage_category,
    
    -- Data quality monitoring
    missing_date_flag,
    future_date_flag,
    missing_order_flag,
    invalid_order_flag,
    (missing_date_flag + future_date_flag + missing_order_flag + invalid_order_flag) AS total_quality_issues
    
FROM cleaned_funnel
