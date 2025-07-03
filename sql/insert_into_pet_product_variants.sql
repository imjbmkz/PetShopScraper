INSERT INTO product_variants (
    product_id
    ,shop_id
    ,url
    ,variant
    ,image_urls
)
SELECT DISTINCT 
    b.id
    ,b.shop_id
    ,a.url
    ,a.variant
    ,a.image_urls
FROM stg_products a 
LEFT JOIN products b ON b.url=a.url
LEFT JOIN pproduct_variants c 
	ON c.url=a.url 
    	-- Replace nulls with empty string to avoid issues with concatenating
    	AND IFNULL(c.variant,'')=IFNULL(a.variant,'')
		AND c.shop_id=b.shop_id
WHERE c.id IS NULL 