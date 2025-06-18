create database ecommerce;
use ecommerce;
SHOW VARIABLES LIKE 'secure_file_priv';

-- Seller Table
CREATE TABLE Seller (
    seller_id CHAR(36) PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city VARCHAR(255),
    seller_state VARCHAR(50)
);

-- Customer Table
CREATE TABLE Customer (
    customer_id CHAR(36) PRIMARY KEY,
    customer_unique_id CHAR(36),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(255),
    customer_state VARCHAR(50)
);


-- Payments Table
CREATE TABLE Payments (
    order_id CHAR(36),
    payment_sequential INT,
    payment_type VARCHAR(100),
    payment_installments INT,
    payment_value FLOAT,
    PRIMARY KEY (order_id, payment_sequential),
    FOREIGN KEY (order_id) REFERENCES Orders(order_id)
);

-- Order Item Table
CREATE TABLE Order_Item (
    order_id CHAR(36),
    order_item_id INT,
    product_id CHAR(36),
    seller_id CHAR(36),
    shipping_limit_date TIMESTAMP,
    price FLOAT,
    freight_value FLOAT,
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
    FOREIGN KEY (product_id) REFERENCES Products(product_id),
    FOREIGN KEY (seller_id) REFERENCES Seller(seller_id)
);

-- Customer Review Table
CREATE TABLE Customer_Review (
    review_id CHAR(36) PRIMARY KEY,
    order_id CHAR(36),
    review_score INT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES Orders(order_id)
);

-- Order Table
CREATE TABLE Orders (
    order_id CHAR(36) PRIMARY KEY,
    customer_id CHAR(36),
    order_status VARCHAR(100),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES Customer(customer_id)
);

-- Products Table
CREATE TABLE Products (
    product_id CHAR(36) PRIMARY KEY,
    product_category_name VARCHAR(255),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm INT,
    product_height_cm INT,
    product_width_cm INT
);
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Products_table_cleaned.csv'
INTO TABLE products
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cleaned_sellers_table.csv'
INTO TABLE seller
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/customers_cleaned.csv'
INTO TABLE customer
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cleaned_orders.csv'
INTO TABLE orders
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cleaned_payments.csv'
INTO TABLE payments
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Order_items_table_cleaned.csv'
INTO TABLE order_item
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Customers_review_table (1).csv'
INTO TABLE customer_review
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

-- 1. order_item.order_id → orders.order_id
ALTER TABLE order_item
ADD CONSTRAINT fk_orderitem_order
FOREIGN KEY (order_id)
REFERENCES orders(order_id);

-- 2. order_item.seller_id → sellers.seller_id
ALTER TABLE order_item
ADD CONSTRAINT fk_orderitem_seller
FOREIGN KEY (seller_id)
REFERENCES seller(seller_id);

-- 3. order_item.product_id → products.product_id
ALTER TABLE order_item
ADD CONSTRAINT fk_orderitem_product
FOREIGN KEY (product_id)
REFERENCES products(product_id);

ALTER TABLE Orders
ADD CONSTRAINT fk_customer_id 
FOREIGN KEY (customer_id)
REFERENCES Customer(customer_id)
ON DELETE CASCADE
ON UPDATE CASCADE;

ALTER TABLE Payments
ADD CONSTRAINT fk_order_id_payments 
FOREIGN KEY (order_id) 
REFERENCES Orders(order_id)
ON DELETE CASCADE
ON UPDATE CASCADE;

ALTER TABLE customer_review
ADD CONSTRAINT fk_order_id_reviews 
FOREIGN KEY (order_id) 
REFERENCES Orders(order_id)
ON DELETE CASCADE
ON UPDATE CASCADE;

ALTER TABLE order_item
ADD CONSTRAINT fk_order_id_items 
FOREIGN KEY (order_id) 
REFERENCES Orders(order_id)
ON DELETE CASCADE
ON UPDATE CASCADE;

-- ECOMMERCE-ANALYSIS
-- 1 Cumulative Revenue and Trends Overtime
-- Extract order and payment details.
-- Aggregate revenue over time (daily, weekly, or monthly).
-- Identify trends such as peak seasons, sales spikes, or revenue drops.
SELECT 
  DATE(order_purchase_timestamp) AS order_date,
  SUM(payment_value) AS daily_revenue
FROM orders o
JOIN payments p ON o.order_id = p.order_id
GROUP BY order_date
ORDER BY order_date;

-- 2. Most Favored Product Categories and Sales Comparison
-- Categorize products based on sales volume.
-- Compare total revenue and the number of orders per category.
-- Visualize category-wise sales performance over time
SELECT 
  pr.product_category_name,
  COUNT(DISTINCT oi.order_id) AS total_orders,
  SUM(oi.price) AS total_sales
FROM order_item oi
JOIN products pr ON oi.product_id = pr.product_id
GROUP BY pr.product_category_name
ORDER BY total_sales DESC;

-- 3. Mean Order Value (AOV) Across Categories and Payment Methods
-- Calculate average order value for all transactions.
-- Analyze variations across different product categories and payment methods.
-- Identify high-value categories and preferred payment options.
SELECT 
  payment_type,
  COUNT(DISTINCT p.order_id) AS total_orders,
  SUM(payment_value) AS total_revenue,
  ROUND(SUM(payment_value)/COUNT(DISTINCT p.order_id), 2) AS AOV
FROM payments p
GROUP BY payment_type;

-- 4. Active Sellers and Their Growth Trends
-- Count unique sellers who have made sales over time.
-- Monitor the fluctuation of seller activity over months/years.
-- Detect trends in seller retention and growth.
SELECT 
  DATE(o.order_purchase_timestamp) AS order_date,
  COUNT(DISTINCT oi.seller_id) AS active_sellers
FROM orders o
JOIN order_item oi ON o.order_id = oi.order_id
GROUP BY order_date
ORDER BY order_date;

-- 5. Seller Ratings and Impact on Sales Performance
-- Analyze seller ratings distribution.
-- Correlate seller ratings with total sales.
-- Determine if higher-rated sellers generate more revenue.
SELECT 
  oi.seller_id,
  AVG(r.review_score) AS avg_rating,
  COUNT(DISTINCT oi.order_id) AS total_orders,
  SUM(oi.price) AS total_sales
FROM order_item oi
JOIN customer_review r ON oi.order_id = r.order_id
GROUP BY oi.seller_id
ORDER BY avg_rating DESC;

-- 6. Repeat Customer Analysis and Sales Contribution
-- Identify customers who made multiple purchases.
-- Compute the percentage of repeat customers.
-- Evaluate their contribution to total revenue and order volume.

-- Step 1: Identify customers with more than one order (i.e., repeat customers)
WITH customer_order_counts AS (
  SELECT 
    customer_id,
    COUNT(order_id) AS total_orders
  FROM orders
  GROUP BY customer_id
),

-- Step 2: Tag them as repeat or not
tagged_customers AS (
  SELECT 
    customer_id,
    total_orders,
    CASE WHEN total_orders > 1 THEN 'Repeat' ELSE 'One-time' END AS customer_type
  FROM customer_order_counts
),

-- Step 3: Join with orders and payments to get revenue
orders_with_type AS (
  SELECT 
    o.order_id,
    t.customer_type,
    p.payment_value
  FROM orders o
  JOIN tagged_customers t ON o.customer_id = t.customer_id
  JOIN payments p ON o.order_id = p.order_id
)

-- Step 4: Aggregate results
SELECT 
  customer_type,
  COUNT(DISTINCT order_id) AS total_orders,
  ROUND(SUM(payment_value), 2) AS total_revenue,
  ROUND(
    100.0 * COUNT(DISTINCT order_id) / (SELECT COUNT(*) FROM orders),
    2
  ) AS order_share_percentage,
  ROUND(
    100.0 * SUM(payment_value) / (SELECT SUM(payment_value) FROM payments),
    2
  ) AS revenue_share_percentage
FROM orders_with_type
GROUP BY customer_type;

-- 6. Mean Customer Ratings and Impact on Sales
-- Aggregate customer review scores for each product.
-- Compare average ratings with product sales performance.
-- Identify whether higher-rated products drive more sales.

-- Step 1: Join orders, order_items, and reviews to connect products with ratings and sales
WITH product_reviews AS (
  SELECT 
    oi.product_id,
    r.review_score,
    oi.price
  FROM order_item oi
  JOIN customer_review r ON oi.order_id = r.order_id
)

-- Step 2: Aggregate at the product level
SELECT 
  pr.product_id,
  p.product_category_name,
  COUNT(*) AS total_reviews,
  ROUND(AVG(pr.review_score), 2) AS avg_rating,
  ROUND(SUM(pr.price), 2) AS total_sales
FROM product_reviews pr
JOIN products p ON pr.product_id = p.product_id
GROUP BY pr.product_id, p.product_category_name
ORDER BY avg_rating DESC, total_sales DESC;

-- 7. Order Cancellation Rates and Seller Performance Impact
-- Compute order cancellation rates for sellers.
-- Determine how cancellations affect revenue and seller trust.
-- Identify sellers with high cancellation rates and potential issues.
SELECT 
  oi.seller_id,
  COUNT(DISTINCT CASE WHEN o.order_status = 'canceled' THEN o.order_id END) AS canceled_orders,
  COUNT(DISTINCT o.order_id) AS total_orders,
  ROUND(
    COUNT(DISTINCT CASE WHEN o.order_status = 'canceled' THEN o.order_id END) * 100.0 /
    COUNT(DISTINCT o.order_id), 2
  ) AS cancellation_rate
FROM order_item oi
JOIN orders o ON oi.order_id = o.order_id
GROUP BY oi.seller_id;

-- 8. Top-Selling Products and Sales Evolution
-- Rank products based on total sales and order count.
-- Monitor their sales trends over time.
-- Identify best-performing products and seasonal demand shifts.
SELECT 
  pr.product_id,
  pr.product_category_name,
  SUM(oi.price) AS total_sales,
  COUNT(DISTINCT oi.order_id) AS total_orders
FROM order_item oi
JOIN products pr ON oi.product_id = pr.product_id
GROUP BY pr.product_id, pr.product_category_name
ORDER BY total_sales DESC
LIMIT 3;

-- 9. Impact of Customer Reviews on Sales and Product Performance
-- Compare review scores with sales performance.
-- Identify whether better-reviewed products generate higher sales.
-- Detect common review patterns that influence buying decisions.
SELECT 
  pr.product_id,
  pr.product_category_name,
  AVG(r.review_score) AS avg_rating,
  COUNT(oi.order_id) AS order_count,
  SUM(oi.price) AS total_sales
FROM order_item oi
JOIN products pr ON oi.product_id = pr.product_id
JOIN customer_review r ON oi.order_id = r.order_id
GROUP BY pr.product_id, pr.product_category_name
ORDER BY avg_rating DESC;







