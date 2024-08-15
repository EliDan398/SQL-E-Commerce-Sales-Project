/*
Eli Daniels
Analysis on Target Sales
Data: https://www.kaggle.com/datasets/devarajv88/target-dataset?select=sellers.csv
*/


-- Get a preliminary look at the data

SELECT TOP 20 *
  FROM [TargetSalesDataset].[dbo].[customers]

SELECT TOP 20 *
  FROM [TargetSalesDataset].[dbo].[orders]

SELECT TOP 20 *
  FROM [TargetSalesDataset].[dbo].[payments]

SELECT TOP 20 *
  FROM [TargetSalesDataset].[dbo].[products]

SELECT TOP 20 *
  FROM [TargetSalesDataset].[dbo].[sellers]

SELECT TOP 20 *
  FROM [TargetSalesDataset].[dbo].[order_items]

SELECT TOP 20 *
  FROM [TargetSalesDataset].[dbo].[geolocation]


/*
Start of the data cleaning process
*/

-- look for and remove duplicate data 

WITH duplicate_entries AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state
ORDER BY customer_id) AS row_num
FROM [TargetSalesDataset].[dbo].[customers]
)
SELECT *
FROM duplicate_entries
WHERE row_num > 1

--no duplicates found for this table
--continue for the other tables

WITH duplicate_entries AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at,
order_delivered_customer_date, order_estimated_delivery_date
ORDER BY order_id) as row_num
FROM [TargetSalesDataset].[dbo].[orders]
)
SELECT *
FROM duplicate_entries
WHERE row_num > 1


WITH duplicate_entries AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY order_id, payment_sequential, payment_type, payment_installments, payment_value
ORDER BY order_id) AS row_num
FROM [TargetSalesDataset].[dbo].[payments]
)
SELECT *
FROM duplicate_entries
WHERE row_num > 1


WITH duplicate_entries AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY product_id, product_category, product_name_length, product_description_length, product_photos_qty,
product_weight_g, product_length_cm, product_height_cm, product_width_cm
ORDER BY product_id) AS row_num
FROM [TargetSalesDataset].[dbo].[products]
)
SELECT *
FROM duplicate_entries
WHERE row_num > 1


WITH duplicate_entries AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY seller_id, seller_zip_code_prefix, seller_city, seller_state
ORDER BY seller_id) AS row_num
FROM [TargetSalesDataset].[dbo].[sellers]
)
SELECT *
FROM duplicate_entries
WHERE row_num > 1


WITH duplicate_entries AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY order_id, order_item_id, product_id, seller_id, shipping_limit_date, freight_value
ORDER BY order_id) AS row_num
FROM [TargetSalesDataset].[dbo].[order_items]
)
SELECT *
FROM duplicate_entries
WHERE row_num > 1



WITH duplicate_entries AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state
ORDER BY geolocation_zip_code_prefix) AS row_num
FROM [TargetSalesDataset].[dbo].[geolocation]
)
SELECT *
FROM duplicate_entries
WHERE row_num > 1

-- Found duplicate data in geolocation table
-- Making a copy of the data and removing duplicates

USE TargetSalesDataset;
CREATE TABLE geolocation_cleaned (
    geolocation_zip_code_prefix INT,
    geolocation_lat FLOAT, 
    geolocation_lng FLOAT,
    geolocation_city NVARCHAR(50),
    geolocation_state NVARCHAR(50)
);

INSERT INTO [TargetSalesDataset].[dbo].[geolocation_cleaned] (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state)
SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state
FROM [TargetSalesDataset].[dbo].[geolocation];


WITH duplicate_entries AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state
ORDER BY geolocation_zip_code_prefix) AS row_num
FROM [TargetSalesDataset].[dbo].[geolocation_cleaned]
)
DELETE FROM duplicate_entries
WHERE row_num > 1

--Fixing inconsistent entries
UPDATE [TargetSalesDataset].[dbo].[geolocation_cleaned]
SET geolocation_city = REPLACE(geolocation_city, 'são paulo', 'sao paulo')
WHERE geolocation_city LIKE '%são paulo%';

SELECT * FROM [TargetSalesDataset].[dbo].[geolocation_cleaned]

/*

End of data cleaning 
Start of data analysis

*/


--number of products for each category
SELECT product_category, COUNT(product_category) AS 'Number of Products'
FROM [TargetSalesDataset].[dbo].[products]
GROUP BY product_category
ORDER BY COUNT(product_category) desc

--Find the most sold products and their category
SELECT items.product_id, products.product_category,COUNT(items.product_id) as 'Number of Orders'
FROM [TargetSalesDataset].[dbo].[order_items] as items
JOIN [TargetSalesDataset].[dbo].[products] as products
ON items.product_id = products.product_id
GROUP BY items.product_id, products.product_category
ORDER BY COUNT(items.product_id) desc


--Find the products with the highest revenue
SELECT product_id, CONCAT('$',FORMAT(ROUND(SUM(price), 2), 'N2')) as 'Revenue Generated'
FROM [TargetSalesDataset].[dbo].[order_items]
GROUP BY product_id
ORDER BY SUM(price) desc


--Find the sellers that sold a total price of over $100,000 to Target
SELECT Sellers.seller_id, CONCAT('$',FORMAT(ROUND(SUM(Orders.price), 2), 'N2')) AS 'Total Order Price'
FROM [TargetSalesDataset].[dbo].[sellers] AS Sellers
JOIN [TargetSalesDataset].[dbo].[order_items] AS Orders on Sellers.seller_id = Orders.seller_id
GROUP BY Sellers.seller_id
HAVING SUM(Orders.price) >= 100000
ORDER BY SUM(Orders.price) DESC


--Find the top 5 cities with the most customers
SELECT TOP 5 customer_city, COUNT(customer_city) AS 'Number of Customers'
FROM [TargetSalesDataset].[dbo].[customers]
GROUP BY customer_city
ORDER BY COUNT(customer_unique_id) desc


--Find the order status of each of the Sellers
SELECT Sellers.seller_id,
COUNT(CASE WHEN Orders.order_status = 'delivered' THEN 1 END) AS Delivered,
COUNT(CASE WHEN Orders.order_status = 'approved' THEN 1 END) AS Approved,
COUNT(CASE WHEN Orders.order_status = 'shipped' THEN 1 END) AS Shipped,
COUNT(CASE WHEN Orders.order_status = 'created' THEN 1 END) AS Created,
COUNT(CASE WHEN Orders.order_status = 'invoiced' THEN 1 END) AS Invoiced,
COUNT(CASE WHEN Orders.order_status = 'processing' THEN 1 END) AS Processing,
COUNT(CASE WHEN Orders.order_status = 'unavailable' THEN 1 END) AS Unavailable,
COUNT(CASE WHEN Orders.order_status = 'canceled' THEN 1 END) AS Canceled
FROM [TargetSalesDataset].[dbo].[orders] AS Orders
JOIN [TargetSalesDataset].[dbo].[order_items] AS Order_items
ON Orders.order_id = Order_items.order_id
JOIN [TargetSalesDataset].[dbo].[sellers] AS Sellers
ON Sellers.seller_id = Order_items.seller_id
GROUP BY Sellers.seller_id
ORDER BY COUNT(CASE WHEN Orders.order_status = 'delivered' THEN 1 END) DESC


--Find the Quarters with the most orders 
SELECT 
	SUM(CASE WHEN MONTH(order_purchase_timestamp) BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS Quarter_1,
    SUM(CASE WHEN MONTH(order_purchase_timestamp) BETWEEN 4 AND 6 THEN 1 ELSE 0 END) AS Quarter_2,
    SUM(CASE WHEN MONTH(order_purchase_timestamp) BETWEEN 7 AND 9 THEN 1 ELSE 0 END) AS Quarter_3,
    SUM(CASE WHEN MONTH(order_purchase_timestamp) BETWEEN 10 AND 12 THEN 1 ELSE 0 END) AS Quarter_4
FROM [TargetSalesDataset].[dbo].[orders]


--Find the Number of payments using a credit or a debit card
SELECT DISTINCT payment_type
FROM [TargetSalesDataset].[dbo].[payments]

SELECT 'Number of Card Payments' AS payment_type, 
SUM(CASE
WHEN payment_type IN ('credit_card', 'debit_card') THEN 1
END
) AS 'Count'
FROM [TargetSalesDataset].[dbo].[payments]


--Find the Percentage of payments using a credit or a debit card
SELECT 'Percentage of Card Payments' AS payment_type, 
ROUND(SUM(CASE WHEN payment_type IN ('credit_card', 'debit_card') THEN 1 END) /
CAST(COUNT(payment_type) AS FLOAT) * 100, 2) AS 'Percentage'
FROM [TargetSalesDataset].[dbo].[payments]
