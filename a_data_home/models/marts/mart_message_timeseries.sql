-- models/marts/mart_message_timeseries.sql
{{ config(materialized='table') }}

WITH message_base AS (
    SELECT
        message_id,
        chat_id,
        message_guid,
        message_text,
        message_date,
        date_read,
        date_delivered,

        -- Contact and conversation info
        partner_display_name,
        first_name,
        last_name,
        contact_phone_number,
        chat_identifier,
        conversation_partner,
        contact_match_type,
        phone_type,

        -- Message metadata
        is_from_me,
        service,
        type,
        cache_has_attachments,
        is_delivered,
        is_read,

        -- Timeline calculations from int_messages
        response_time_minutes,
        read_time_minutes,
        delivery_time_seconds,
        is_consecutive_message,
        is_conversation_starter,
        message_sequence,

        -- Time dimensions for analysis
        DATE_TRUNC('hour', message_date) AS message_hour,
        DATE_TRUNC('day', message_date) AS message_date_day,
        DATE_TRUNC('week', message_date) AS message_week,
        DATE_TRUNC('month', message_date) AS message_month,
        DATE_TRUNC('quarter', message_date) AS message_quarter,
        DATE_TRUNC('year', message_date) AS message_year,

        -- Time components
        EXTRACT(HOUR FROM message_date) AS hour_of_day,
        EXTRACT(DOW FROM message_date) AS day_of_week,  -- 0=Sunday
        EXTRACT(DAY FROM message_date) AS day_of_month,
        EXTRACT(MONTH FROM message_date) AS month_of_year,
        EXTRACT(YEAR FROM message_date) AS year,

        -- Time classifications
        CASE
            WHEN EXTRACT(HOUR FROM message_date) BETWEEN 6 AND 11 THEN 'morning'
            WHEN EXTRACT(HOUR FROM message_date) BETWEEN 12 AND 17 THEN 'afternoon'
            WHEN EXTRACT(HOUR FROM message_date) BETWEEN 18 AND 21 THEN 'evening'
            ELSE 'night'
        END AS time_of_day,

        CASE
            WHEN EXTRACT(DOW FROM message_date) IN (0, 6) THEN 'weekend'
            ELSE 'weekday'
        END AS day_type,

        CASE
            WHEN EXTRACT(DOW FROM message_date) = 0 THEN 'Sunday'
            WHEN EXTRACT(DOW FROM message_date) = 1 THEN 'Monday'
            WHEN EXTRACT(DOW FROM message_date) = 2 THEN 'Tuesday'
            WHEN EXTRACT(DOW FROM message_date) = 3 THEN 'Wednesday'
            WHEN EXTRACT(DOW FROM message_date) = 4 THEN 'Thursday'
            WHEN EXTRACT(DOW FROM message_date) = 5 THEN 'Friday'
            WHEN EXTRACT(DOW FROM message_date) = 6 THEN 'Saturday'
        END AS day_name

    FROM {{ ref('int_messages') }}
    WHERE partner_display_name IS NOT null  -- Only include conversations with known contacts
),

windowed_metrics AS (
    SELECT
        *,

        -- Previous/Next message context within conversation
        LAG(message_date, 1) OVER (
            PARTITION BY chat_id
            ORDER BY message_date
        ) AS prev_message_date,
        LAG(is_from_me, 1) OVER (
            PARTITION BY chat_id
            ORDER BY message_date
        ) AS prev_message_from_me,
        LAG(partner_display_name, 1) OVER (
            PARTITION BY chat_id
            ORDER BY message_date
        ) AS prev_message_sender,

        LEAD(message_date, 1) OVER (
            PARTITION BY chat_id
            ORDER BY message_date
        ) AS next_message_date,
        LEAD(is_from_me, 1) OVER (
            PARTITION BY chat_id
            ORDER BY message_date
        ) AS next_message_from_me,

        -- Time gaps between messages
        DATEDIFF(
            'minute',
            LAG(message_date, 1) OVER (
                PARTITION BY chat_id
                ORDER BY message_date
            ),
            message_date
        ) AS minutes_since_prev_message,

        DATEDIFF(
            'minute',
            message_date,
            LEAD(message_date, 1) OVER (
                PARTITION BY chat_id
                ORDER BY message_date
            )
        ) AS minutes_until_next_message,

        -- Daily position within conversation
        ROW_NUMBER() OVER (
            PARTITION BY chat_id, message_date_day
            ORDER BY message_date
        ) AS daily_message_sequence,

        -- Running totals within conversation
        COUNT(*) OVER (
            PARTITION BY chat_id
            ORDER BY message_date
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_messages_in_conversation,

        COUNT(CASE WHEN is_from_me THEN 1 END) OVER (
            PARTITION BY chat_id
            ORDER BY message_date
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_sent_in_conversation,

        COUNT(CASE WHEN NOT is_from_me THEN 1 END) OVER (
            PARTITION BY chat_id
            ORDER BY message_date
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_received_in_conversation,

        -- Rolling windows for pattern analysis
        COUNT(*) OVER (
            PARTITION BY chat_id
            ORDER BY message_date
            ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
        ) AS messages_last_10,

        AVG(LENGTH(message_text)) OVER (
            PARTITION BY chat_id
            ORDER BY message_date
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS avg_length_last_5_messages

    FROM message_base
),

enriched_timeseries AS (
    SELECT
        *,

        -- Message timing patterns
        CASE
            WHEN minutes_since_prev_message IS null THEN 'conversation_start'
            WHEN minutes_since_prev_message <= 2 THEN 'immediate_follow_up'
            WHEN minutes_since_prev_message <= 15 THEN 'quick_response'
            WHEN minutes_since_prev_message <= 60 THEN 'within_hour'
            WHEN minutes_since_prev_message <= 1440 THEN 'within_day'  -- 24 hours
            ELSE 'delayed_response'
        END AS response_timing_category,

        -- Conversation momentum indicators
        CASE
            WHEN messages_last_10 >= 8 THEN 'high_momentum'
            WHEN messages_last_10 >= 4 THEN 'medium_momentum'
            WHEN messages_last_10 >= 2 THEN 'low_momentum'
            ELSE 'minimal_momentum'
        END AS conversation_momentum,

        -- Message streak detection
        COALESCE(is_consecutive_message AND prev_message_from_me = is_from_me, false) AS is_part_of_streak,

        -- Session detection (messages within 30 minutes = same session)
        COALESCE(minutes_since_prev_message IS null OR minutes_since_prev_message > 30, false) AS is_session_start,

        -- Daily first/last message indicators
        COALESCE(daily_message_sequence = 1, false) AS is_first_daily_message,

        -- Message length classification
        CASE
            WHEN LENGTH(message_text) <= 10 THEN 'very_short'
            WHEN LENGTH(message_text) <= 50 THEN 'short'
            WHEN LENGTH(message_text) <= 200 THEN 'medium'
            WHEN LENGTH(message_text) <= 500 THEN 'long'
            ELSE 'very_long'
        END AS message_length_category,

        -- Communication intensity (messages per day in this conversation)
        cumulative_messages_in_conversation::FLOAT
        / (DATEDIFF(
            'day',
            FIRST_VALUE(message_date) OVER (
                PARTITION BY chat_id
                ORDER BY message_date
            ),
            message_date
        ) + 1) AS messages_per_day_to_date

    FROM windowed_metrics
)

SELECT
    -- Primary identifiers
    message_id,
    chat_id,
    message_guid,

    -- Contact information (flattened for easy access)
    first_name,
    last_name,
    contact_phone_number,
    chat_identifier,
    contact_match_type,
    phone_type,
    message_text,

    -- Message content and metadata
    message_length_category,
    is_from_me,
    message_date,
    message_hour,
    message_date_day,

    -- Timing information (multiple granularities)
    message_week,
    message_month,
    message_quarter,
    message_year,
    hour_of_day,
    day_of_week,
    day_of_month,
    month_of_year,
    year,
    time_of_day,
    day_type,
    day_name,
    message_sequence,
    daily_message_sequence,
    cumulative_messages_in_conversation,

    -- Message sequence and context
    cumulative_sent_in_conversation,
    cumulative_received_in_conversation,
    response_time_minutes,
    read_time_minutes,
    minutes_since_prev_message,

    -- Timing patterns and conversation flow
    minutes_until_next_message,
    response_timing_category,
    conversation_momentum,
    is_conversation_starter,
    is_consecutive_message,
    is_part_of_streak,
    is_session_start,
    is_first_daily_message,
    date_delivered,
    date_read,
    is_delivered,

    -- Delivery and read status
    is_read,
    delivery_time_seconds,
    service,
    type,
    cache_has_attachments,

    -- Message characteristics
    messages_last_10,
    avg_length_last_5_messages,
    messages_per_day_to_date,

    -- Aggregated context
    COALESCE(partner_display_name, chat_identifier) AS contact_name,
    LENGTH(message_text) AS message_length,
    CASE WHEN is_from_me THEN 'sent' ELSE 'received' END AS message_direction,

    -- LLM-friendly formatted fields
    CONCAT(
        CASE WHEN is_from_me THEN 'Me' ELSE contact_name END,
        ' at ',
        STRFTIME(message_date, '%Y-%m-%d %H:%M'),
        ' (', time_of_day, ', ', day_name, '): ',
        LEFT(message_text, 200),
        CASE WHEN LENGTH(message_text) > 200 THEN '...' ELSE '' END
    ) AS formatted_message,

    -- Temporal context for LLM
    CONCAT(
        'Message ', message_sequence, ' in conversation with ', contact_name,
        '. Sent on ', day_name, ' at ', time_of_day,
        CASE
            WHEN response_time_minutes IS NOT null
                THEN CONCAT(' (', response_time_minutes, ' min response time)')
            ELSE ''
        END
    ) AS temporal_context

FROM enriched_timeseries
WHERE first_name = 'Bek'
ORDER BY message_date DESC
