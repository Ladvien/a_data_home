-- models/marts/mart_conversations.sql
{{ config(materialized='table') }}

WITH conversation_base AS (
    SELECT 
        chat_id,
        chat_identifier,
        partner_display_name,
        contact_match_type,
        first_name,
        last_name,
        phone_type,

        -- Conversation classification
        CASE
            WHEN chat_identifier LIKE 'chat%' OR display_name IS NOT NULL THEN 'group_chat'
            WHEN contact_match_type = 'email' THEN 'email_conversation'
            WHEN contact_match_type = 'short_code' THEN 'business_sms'
            ELSE 'individual_conversation'
        END AS conversation_type,

        -- Message aggregations
        COUNT(*) AS total_messages,
        COUNT(CASE WHEN is_from_me THEN 1 END) AS messages_sent,
        COUNT(CASE WHEN NOT is_from_me THEN 1 END) AS messages_received,

        -- Timeline metrics
        MIN(message_date) AS first_message_date,
        MAX(message_date) AS last_message_date,
        MAX(CASE WHEN is_from_me THEN message_date END) AS last_sent_date,
        MAX(CASE WHEN NOT is_from_me THEN message_date END) AS last_received_date,

        -- Conversation activity
        DATEDIFF('day', MIN(message_date), MAX(message_date)) AS conversation_duration_days,
        COUNT(DISTINCT DATE_TRUNC('day', message_date)) AS active_days,
        COUNT(DISTINCT DATE_TRUNC('week', message_date)) AS active_weeks,
        COUNT(DISTINCT DATE_TRUNC('month', message_date)) AS active_months,
        
        -- Response patterns
        AVG(CASE WHEN response_time_minutes IS NOT NULL THEN response_time_minutes END) AS avg_response_time_minutes,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY response_time_minutes) AS median_response_time_minutes,
        AVG(CASE WHEN read_time_minutes IS NOT NULL THEN read_time_minutes END) AS avg_read_time_minutes,
        
        -- Message characteristics
        AVG(LENGTH(message_text)) AS avg_message_length,
        COUNT(CASE WHEN cache_has_attachments THEN 1 END) AS messages_with_attachments,
        COUNT(CASE WHEN service = 'iMessage' THEN 1 END) AS imessage_count,
        COUNT(CASE WHEN service = 'SMS' THEN 1 END) AS sms_count,
        
        -- Delivery and read metrics
        AVG(CASE WHEN delivery_time_seconds IS NOT NULL THEN delivery_time_seconds END) AS avg_delivery_time_seconds,
        COUNT(CASE WHEN is_read AND NOT is_from_me THEN 1 END) AS received_messages_read,
        COUNT(CASE WHEN NOT is_from_me THEN 1 END) AS total_received_messages,
        
        -- Conversation flow patterns
        COUNT(CASE WHEN is_consecutive_message THEN 1 END) AS consecutive_message_count,
        MAX(CASE WHEN is_from_me THEN message_sequence END) AS last_sent_sequence,
        MAX(CASE WHEN NOT is_from_me THEN message_sequence END) AS last_received_sequence
        
    FROM {{ ref('int_messages') }}
    GROUP BY 1,2,3,4,5,6,7,8
),

conversation_enriched AS (
    SELECT 
        *,
        
        -- Calculated ratios and derived metrics
        CASE 
            WHEN total_messages > 0 
            THEN messages_sent::FLOAT / total_messages 
            ELSE 0 
        END AS sent_ratio,
        
        CASE 
            WHEN messages_received > 0 
            THEN messages_sent::FLOAT / messages_received 
            ELSE NULL 
        END AS sent_received_ratio,
        
        CASE 
            WHEN conversation_duration_days > 0 
            THEN total_messages::FLOAT / conversation_duration_days 
            ELSE total_messages 
        END AS messages_per_day,
        
        CASE 
            WHEN active_days > 0 
            THEN total_messages::FLOAT / active_days 
            ELSE total_messages 
        END AS messages_per_active_day,
        
        CASE 
            WHEN total_received_messages > 0 
            THEN received_messages_read::FLOAT / total_received_messages 
            ELSE 0 
        END AS read_rate,
        
        CASE 
            WHEN total_messages > 0 
            THEN messages_with_attachments::FLOAT / total_messages 
            ELSE 0 
        END AS attachment_rate,
        
        -- Conversation activity classification
        CASE
            WHEN last_message_date >= CURRENT_DATE - INTERVAL 7 DAYS THEN 'very_active'
            WHEN last_message_date >= CURRENT_DATE - INTERVAL 30 DAYS THEN 'active'
            WHEN last_message_date >= CURRENT_DATE - INTERVAL 90 DAYS THEN 'moderately_active'
            WHEN last_message_date >= CURRENT_DATE - INTERVAL 365 DAYS THEN 'inactive'
            ELSE 'dormant'
        END AS activity_status,
        
        -- Conversation engagement level
        CASE
            WHEN total_messages >= 1000 AND messages_per_active_day >= 10 THEN 'high_engagement'
            WHEN total_messages >= 100 AND messages_per_active_day >= 5 THEN 'medium_engagement'
            WHEN total_messages >= 10 THEN 'low_engagement'
            ELSE 'minimal_engagement'
        END AS engagement_level,
        
        -- Response behavior classification
        CASE
            WHEN avg_response_time_minutes <= 5 THEN 'immediate_responder'
            WHEN avg_response_time_minutes <= 60 THEN 'quick_responder'
            WHEN avg_response_time_minutes <= 1440 THEN 'daily_responder'  -- 24 hours
            ELSE 'slow_responder'
        END AS response_behavior,
        
        -- Communication preference
        CASE
            WHEN imessage_count::FLOAT / total_messages > 0.8 THEN 'prefers_imessage'
            WHEN sms_count::FLOAT / total_messages > 0.8 THEN 'prefers_sms'
            ELSE 'mixed_messaging'
        END AS messaging_preference,
        
        -- Recent activity indicators
        CASE WHEN last_sent_date > last_received_date THEN TRUE ELSE FALSE END AS last_message_was_sent,
        DATEDIFF('day', last_message_date, CURRENT_DATE) AS days_since_last_message,
        DATEDIFF('day', last_sent_date, CURRENT_DATE) AS days_since_last_sent,
        DATEDIFF('day', last_received_date, CURRENT_DATE) AS days_since_last_received
        
    FROM conversation_base
),

-- Add recent message samples for context
recent_messages AS (
    SELECT 
        chat_id,
        STRING_AGG(
            CASE WHEN is_from_me THEN 'Me: ' ELSE partner_display_name || ': ' END || 
            LEFT(message_text, 100) || 
            CASE WHEN LENGTH(message_text) > 100 THEN '...' ELSE '' END,
            ' | '
            ORDER BY message_date DESC
        ) AS recent_message_preview
    FROM (
        SELECT 
            chat_id,
            partner_display_name,
            message_text,
            message_date,
            is_from_me,
            ROW_NUMBER() OVER (PARTITION BY chat_id ORDER BY message_date DESC) AS rn
        FROM {{ ref('int_messages') }}
        WHERE message_text IS NOT NULL 
        AND TRIM(message_text) != ''
    ) 
    WHERE rn <= 3  -- Last 3 messages for context
    GROUP BY chat_id
)

SELECT 
    ce.*,
    rm.recent_message_preview,
    
    -- Summary fields for LLM consumption
    CONCAT(
        'Conversation with ', partner_display_name, 
        ' (', conversation_type, '): ',
        total_messages, ' messages over ', conversation_duration_days, ' days. ',
        'Activity: ', activity_status, ', Engagement: ', engagement_level, ', ',
        'Response time: ', response_behavior, '. ',
        'Last message: ', days_since_last_message, ' days ago.'
    ) AS conversation_summary,
    
    -- Prioritization score for most important conversations
    (
        (total_messages / 100.0) * 0.3 +  -- Volume weight
        (CASE WHEN activity_status = 'very_active' THEN 1.0 
              WHEN activity_status = 'active' THEN 0.7 
              ELSE 0.3 END) * 0.4 +  -- Recency weight
        (messages_per_active_day / 20.0) * 0.3  -- Intensity weight
    ) AS conversation_importance_score

FROM conversation_enriched ce
LEFT JOIN recent_messages rm ON ce.chat_id = rm.chat_id
WHERE conversation_type = 'individual_conversation'  -- Focus on 1:1 conversations
ORDER BY conversation_importance_score DESC