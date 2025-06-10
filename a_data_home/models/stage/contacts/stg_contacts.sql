{{ config(materialized='table') }}
SELECT DISTINCT
    zabcdrecord.zfirstname AS first_name,
    zabcdrecord.zlastname AS last_name,
    zabcdphonenumber.zfullnumber AS full_number
FROM {{ generate_source('address_book', 'ZABCDRECORD') }}       AS zabcdrecord
LEFT JOIN         {{ generate_source('address_book', 'ZABCDPHONENUMBER') }}  AS zabcdphonenumber
    ON zabcdrecord.z_pk = zabcdphonenumber.zowner
ORDER BY
    zabcdrecord.zlastname ASC,
    zabcdrecord.zfirstname ASC,
    zabcdphonenumber.zorderingindex ASC
