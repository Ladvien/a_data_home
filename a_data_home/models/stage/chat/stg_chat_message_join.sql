SELECT
    chat_id,
    message_id,
    message_date
FROM {{ generate_source('chat', 'chat_message_join') }}
