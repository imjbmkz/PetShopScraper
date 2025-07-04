CREATE TABLE {table_name} (
    shop varchar(50) CHARACTER SET utf8mb4,
    name varchar(255) CHARACTER SET utf8mb4,
    rating varchar(50) CHARACTER SET utf8mb4,
    description text CHARACTER SET utf8mb4,
    url varchar(255) CHARACTER SET utf8mb4,
    variant varchar(255) CHARACTER SET utf8mb4,
    image_urls varchar(1000) CHARACTER SET utf8mb4,
    price decimal(10, 4),
    discounted_price decimal(10, 4),
    discount_percentage decimal(10, 4)
);