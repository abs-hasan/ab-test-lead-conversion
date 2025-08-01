{{ config(materialized='view') }}

-- Stage contact events with DATA CLEANING and VALIDATION
WITH raw_data AS (
    SELECT * FROM {{ source('main', 'contact_events') }}
),

cleaned_contacts AS (
    SELECT 
        event_id,
        lead_id,
        
        event_date AS event_date_clean,
        
        -- STANDARDIZE CONTACT TYPES: Fix inconsistent casing and typos
        CASE 
            WHEN LOWER(TRIM(contact_type)) IN ('email', 'Email') THEN 'Email'
            WHEN LOWER(TRIM(contact_type)) IN ('call', 'phone call', 'Phone Call') THEN 'Phone Call'  
            WHEN LOWER(TRIM(contact_type)) IN ('linkedin', 'LinkedIn', 'LinkedIn Message') THEN 'LinkedIn Message'
            WHEN LOWER(TRIM(contact_type)) = 'demo request' THEN 'Demo Request'
            ELSE 'Other'
        END AS contact_type_clean,
        
        -- CLEAN RESPONSE TYPES: Standardize missing values
        CASE 
            WHEN response_type IS NULL OR TRIM(response_type) = '' THEN 'No Response'
            ELSE TRIM(response_type)
        END AS response_type_clean,
        
        -- ADD DATA QUALITY FLAGS
        CASE WHEN event_date IS NULL THEN 1 ELSE 0 END AS missing_date_flag,

        CASE WHEN response_type IS NULL THEN 1 ELSE 0 END AS missing_response_flag
        
    FROM raw_data
    WHERE lead_id IS NOT NULL  -- Remove records without lead_id
)

SELECT 
    event_id,
    lead_id,
    event_date_clean AS event_date,
    contact_type_clean AS contact_type,
    response_type_clean AS response_type,
    
    -- Data quality monitoring
    missing_date_flag,

    missing_response_flag,
    (missing_date_flag + missing_response_flag) AS total_quality_issues
    
FROM cleaned_contacts
