{{ config(materialized='view') }}

-- Stage leads with DATA CLEANING and VALIDATION
-- Based on data quality analysis and cleaning solutions developed

WITH raw_data AS (
    SELECT * FROM {{ source('main', 'leads') }}
),

-- EMAIL DEDUPLICATION: Keep only the latest lead per email address
deduplicated_leads AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY contact_email 
            ORDER BY created_at DESC, lead_id DESC
        ) as email_rank
    FROM raw_data
    WHERE lead_id IS NOT NULL
      AND contact_email IS NOT NULL
      -- FILTER OUT TEST DATA: Remove completely from dataset
      AND NOT (
          LOWER(company_name) LIKE '%test%' 
          OR LOWER(company_name) LIKE '%delete%'
          OR LOWER(company_name) LIKE '%sample%'
      )
),

cleaned_leads AS (
    SELECT 
        lead_id,
        
        -- COMPANY NAME CLEANING: Remove test data, preserve authentic business formats
        CASE 
            WHEN LOWER(company_name) LIKE '%test%' 
                OR LOWER(company_name) LIKE '%delete%'
                OR LOWER(company_name) LIKE '%sample%'
            THEN NULL
            ELSE TRIM(company_name)
        END AS company_name_clean,
        
        contact_email,
        
        -- PHONE NUMBER CLEANING: Remove extensions and special characters, keep all digits
        CASE 
            WHEN contact_phone IS NULL THEN NULL
            WHEN LENGTH(TRIM(contact_phone)) < 10 THEN NULL
            ELSE REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                CASE WHEN contact_phone LIKE '%x%' 
                     THEN SUBSTR(contact_phone, 1, INSTR(contact_phone, 'x') - 1)
                     WHEN contact_phone LIKE '%ext%'
                     THEN SUBSTR(contact_phone, 1, INSTR(contact_phone, 'ext') - 1)
                     ELSE contact_phone END,
                '(', ''), ')', ''), '-', ''), '.', ''), '+', ''), ' ', '')
        END AS contact_phone_clean,
        
        industry,
        
        -- REGION CLEANING: Handle missing regions
        CASE 
            WHEN region IS NULL OR TRIM(region) = '' THEN 'Unknown'
            ELSE TRIM(region)
        END AS region_clean,
        
        source_channel,
        
        -- COMPANY SIZE STANDARDIZATION: Handle case variations
        CASE 
            WHEN LOWER(TRIM(company_size)) = 'small' THEN 'Small'
            WHEN LOWER(TRIM(company_size)) IN ('medium', 'mid-size') THEN 'Medium'
            WHEN LOWER(TRIM(company_size)) = 'large' THEN 'Large'
            WHEN LOWER(TRIM(company_size)) = 'enterprise' THEN 'Enterprise'
            ELSE 'Unknown'
        END AS company_size_clean,
        
        created_at AS created_at_clean,
        
        -- REVENUE CLEANING: Cap extreme values, handle negatives, round to 2 decimals
        CASE 
            WHEN annual_revenue IS NULL THEN 0.00
            WHEN annual_revenue < 0 THEN 0.00  -- Fix negative revenues
            WHEN annual_revenue > 500000000 THEN 500000000.00  -- Cap at $500M
            ELSE ROUND(annual_revenue, 2)  -- Round to 2 decimal places for consistency
        END AS annual_revenue_clean,
        
        "group" as lead_group,
        assigned_at,
        
        -- DATA QUALITY FLAGS: Track what was cleaned for monitoring
        CASE 
            WHEN LOWER(company_name) LIKE '%test%' 
                OR LOWER(company_name) LIKE '%delete%'
                OR LOWER(company_name) LIKE '%sample%'
            THEN 1 
            ELSE 0 
        END as was_test_data,
        
        CASE 
            WHEN annual_revenue < 0 OR annual_revenue > 500000000 
            THEN 1 
            ELSE 0 
        END as had_revenue_outlier,
        

        
        CASE 
            WHEN contact_phone LIKE '%x%' OR contact_phone LIKE '%ext%'
            THEN 1 
            ELSE 0 
        END as had_phone_extension,
        
        -- EMAIL DEDUPLICATION FLAG: Track if this was a duplicate email
        CASE 
            WHEN email_rank = 1 THEN 0
            ELSE 1
        END as was_duplicate_email
        
    FROM deduplicated_leads
    WHERE email_rank = 1  -- Keep only the latest lead per email
)

SELECT * FROM cleaned_leads
