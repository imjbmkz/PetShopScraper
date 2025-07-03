INSERT INTO urls (
    shop_id
    ,url
    ,updated_date
)
SELECT DISTINCT
    s.id AS shop_id,
    a.url,
    a.updated_date
FROM stg_urls a
JOIN shops s ON s.name = a.shop
LEFT JOIN urls b ON b.url = a.url
WHERE b.id IS NULL;