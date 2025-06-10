-- models/intermediate/int_messages.sql
WITH base_messages AS (
    SELECT
        msg.rowid AS message_id,
        cmj.chat_id,
        msg.guid AS message_guid,
        msg.text,
        msg.is_from_me,
        {{ read_apple_date('msg.date') }} AS message_date,
        IF(
            msg.date_read != 0,
            {{ read_apple_date('msg.date_read') }}, 
            null
        ) AS date_read,
        IF(
            msg.date_delivered != 0,
            {{ read_apple_date('msg.date_delivered') }}, 
            null
        ) AS date_delivered,
        msg.is_delivered,
        msg.is_read,
        msg.is_system_message,
        msg.service,
        msg.type,
        msg.cache_has_attachments,
        chat.chat_identifier,
        chat.display_name,
        -- Normalize chat identifier for contact matching
        REGEXP_REPLACE(chat.chat_identifier, '[^0-9]', '', 'g') AS chat_clean_digits,
        CASE
            WHEN REGEXP_REPLACE(chat.chat_identifier, '[^0-9]', '', 'g') LIKE '1%' 
                AND LENGTH(REGEXP_REPLACE(chat.chat_identifier, '[^0-9]', '', 'g')) = 11
                THEN SUBSTRING(REGEXP_REPLACE(chat.chat_identifier, '[^0-9]', '', 'g'), 2)
            ELSE REGEXP_REPLACE(chat.chat_identifier, '[^0-9]', '', 'g')
        END AS chat_normalized_number,
        CASE
            WHEN chat.chat_identifier LIKE '+1%'
                THEN SUBSTRING(chat.chat_identifier, 3)
            WHEN chat.chat_identifier LIKE '+%'
                THEN chat.chat_identifier
            ELSE chat.chat_identifier
        END AS conversation_partner
    FROM {{ ref('stg_message') }} AS msg
    INNER JOIN {{ ref('stg_chat_message_join') }} AS cmj 
        ON msg.rowid = cmj.message_id
    INNER JOIN {{ ref('stg_chat') }} AS chat 
        ON cmj.chat_id = chat.row_id
    WHERE msg.text IS NOT NULL 
        AND msg.text != ''
        AND NOT msg.is_system_message
),

messages_with_contacts AS (
    SELECT 
        bm.*,
        -- Contact information (works for both sent and received messages)
        nc.first_name,
        nc.last_name,
        nc.contact_display_name,
        nc.full_number AS contact_phone_number,
        nc.phone_type,
        nc.contact_priority,
        -- Determine conversation partner name (same logic for both directions)
        CASE
            WHEN TRIM(bm.display_name) != '' AND bm.display_name IS NOT NULL THEN bm.display_name
            WHEN nc.contact_display_name IS NOT NULL THEN nc.contact_display_name
            ELSE bm.chat_identifier
        END AS partner_display_name,
        -- Contact match quality
        CASE
            WHEN nc.normalized_number IS NOT NULL THEN 'exact_match'
            WHEN bm.chat_identifier LIKE '%@%' THEN 'email'
            WHEN LENGTH(bm.chat_clean_digits) < 10 THEN 'short_code'
            ELSE 'unknown_number'
        END AS contact_match_type
    FROM base_messages bm
    LEFT JOIN {{ ref('int_contacts_normalized') }} nc
        ON bm.chat_normalized_number = nc.normalized_number
        AND bm.chat_identifier NOT LIKE '%@%'  -- Don't try to match emails
        AND bm.chat_identifier NOT LIKE 'chat%'  -- Don't try to match group identifiers
        -- This join works for BOTH sent and received messages
        -- because chat_identifier represents the OTHER person in the conversation
),

message_with_context AS (
    SELECT
        *,
        LAG(message_date) OVER (
            PARTITION BY chat_id 
            ORDER BY message_date
        ) AS prev_message_date,
        LAG(is_from_me) OVER (
            PARTITION BY chat_id 
            ORDER BY message_date
        ) AS prev_is_from_me,
        LEAD(message_date) OVER (
            PARTITION BY chat_id 
            ORDER BY message_date
        ) AS next_message_date,
        LEAD(is_from_me) OVER (
            PARTITION BY chat_id 
            ORDER BY message_date
        ) AS next_is_from_me,
        ROW_NUMBER() OVER (
            PARTITION BY chat_id 
            ORDER BY message_date
        ) AS message_sequence
    FROM messages_with_contacts
)

SELECT
    message_id,
    chat_id,
    message_guid,
    text AS message_text,
    is_from_me,
    message_date,
    date_read,
    date_delivered,
    is_delivered,
    is_read,
    service,
    type,
    cache_has_attachments,
    chat_identifier,
    display_name,
    conversation_partner,
    
    -- Contact information
    first_name,
    last_name,
    contact_display_name,
    contact_phone_number,
    phone_type,
    contact_priority,
    partner_display_name,
    contact_match_type,
    
    message_sequence,
    
    -- Timeline calculations
    CASE
        WHEN prev_is_from_me != is_from_me AND prev_message_date IS NOT NULL
        THEN DATEDIFF('minute', prev_message_date, message_date)
        ELSE NULL
    END AS response_time_minutes,
    
    CASE
        WHEN NOT is_from_me AND date_read IS NOT NULL
        THEN DATEDIFF('minute', message_date, date_read)
        ELSE NULL
    END AS read_time_minutes,
    
    CASE
        WHEN is_from_me AND date_delivered IS NOT NULL
        THEN DATEDIFF('second', message_date, date_delivered)
        ELSE NULL
    END AS delivery_time_seconds,
    
    -- Conversation flow indicators
    CASE
        WHEN prev_is_from_me = is_from_me THEN TRUE
        ELSE FALSE
    END AS is_consecutive_message,
    
    CASE
        WHEN message_sequence = 1 THEN TRUE
        ELSE FALSE
    END AS is_conversation_starter

FROM message_with_context
ORDER BY chat_id, message_date