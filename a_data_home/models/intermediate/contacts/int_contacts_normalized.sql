-- models/intermediate/contacts/int_contacts_normalized.sql
{{ config(materialized='table') }}

SELECT
    first_name,
    last_name,
    full_number,

    -- Normalize phone numbers for matching
    REGEXP_REPLACE(full_number, '[^0-9]', '', 'g') AS clean_digits,

    CASE
        -- Handle US numbers with +1 prefix
        WHEN
            REGEXP_REPLACE(full_number, '[^0-9]', '', 'g') LIKE '1%'
            AND LENGTH(REGEXP_REPLACE(full_number, '[^0-9]', '', 'g')) = 11
            THEN SUBSTRING(REGEXP_REPLACE(full_number, '[^0-9]', '', 'g'), 2)
        ELSE REGEXP_REPLACE(full_number, '[^0-9]', '', 'g')
    END AS normalized_number,

    -- Create display name with fallback logic
    CASE
        WHEN first_name IS NOT null AND last_name IS NOT null
            THEN CONCAT(first_name, ' ', last_name)
        WHEN first_name IS NOT null
            THEN first_name
        WHEN last_name IS NOT null
            THEN last_name
    END AS contact_display_name,

    -- Phone number type classification
    CASE
        WHEN full_number LIKE '+1%' THEN 'us_number'
        WHEN full_number LIKE '+%' THEN 'international'
        WHEN REGEXP_FULL_MATCH(full_number, '^\d{10}$') THEN 'us_local'
        WHEN REGEXP_FULL_MATCH(full_number, '^\d{4,6}$') THEN 'short_code'
        ELSE 'other'
    END AS phone_type,

    -- Additional metadata
    LENGTH(REGEXP_REPLACE(full_number, '[^0-9]', '', 'g')) AS digit_count,

    -- Priority for duplicate handling (prefer numbers with names)
    CASE
        WHEN first_name IS NOT null OR last_name IS NOT null THEN 1
        ELSE 2
    END AS contact_priority

FROM {{ ref('stg_contacts') }}
WHERE
    full_number IS NOT null
    AND TRIM(full_number) != ''
    AND LENGTH(REGEXP_REPLACE(full_number, '[^0-9]', '', 'g')) >= 4  -- Filter out obviously invalid numbers
