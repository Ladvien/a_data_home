SELECT
    rowid AS row_id,
    guid,
    text,
    attributed_body
FROM {{ generate_source('dev', 'attributed_body_cleaned') }}
