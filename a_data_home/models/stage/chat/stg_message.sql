SELECT *
FROM {{ generate_source('chat', 'message') }} AS msg
