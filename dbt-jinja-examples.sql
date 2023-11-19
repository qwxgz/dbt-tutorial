-- models/my_model.sql

{{ config(
  materialized='table',
  tags=['example']
) }}

SELECT
  column1,
  column2
FROM
  my_source_table
  
-- In this example, the SQL code is embedded within a file under the models directory in a dbt project. 
-- The {{ ... }} and {% ... %} denote Jinja macros that allow you to reference other dbt objects and properties, as well as define configurations for the model such as materialization and tags.

  
-- Using the ref macro to reference other dbt models:
SELECT
  column1,
  column2
FROM
  {{ ref('my_source_model') }}
WHERE
  condition = 'example'


-- Defining model-specific configurations using the config macro:
{{ config(
  materialized='table',
  tags=['dimension']
) }}

SELECT
  column1,
  column2
FROM
  my_source_table


-- Incorporating conditional logic with the if macro:
{% if target.name == 'dev' %}
SELECT
  column1,
  column2
FROM
  dev_table
{% else %}
SELECT
  column1,
  column2
FROM
  prod_table
{% endif %}
===========================================================================================================
-- In dbt, you can use the standard SQL GROUP BY and aggregate functions to perform grouping and aggregation operations in your models. 
-- Here's an example of how you can use GROUP BY and aggregate functions in dbt:
-- models/my_aggregate_model.sql

{{ config(
  materialized='table',
  tags=['example']
) }}

SELECT
  category,
  COUNT(*) AS num_products,
  AVG(price) AS avg_price,
  SUM(revenue) AS total_revenue
FROM
  my_products_table
GROUP BY
  category
  
======================================================================================================================

-- To determine the top sales category for products using the Order and Product tables in dbt, you can write a SQL query that calculates the total sales for each product category and identifies the category with the highest sales. 

-- models/top_sales_category.sql

{{ config(
  materialized='table',
  tags=['example']
) }}

WITH category_sales AS (
  SELECT
    p.category,
    SUM(o.quantity * p.price) AS total_sales
  FROM
    orders o
    JOIN products p ON o.product_id = p.id
  GROUP BY
    p.category
)
SELECT
  category,
  total_sales
FROM
  category_sales
WHERE
  total_sales = (SELECT MAX(total_sales) FROM category_sales)
  
-- In this example, we're using a common table expression (CTE) to calculate the total sales for each product category. 
  -- We then select the category with the highest total sales by comparing the total_sales with the maximum total sales from the category_sales CTE.
-- This SQL code can be included in a file under the models directory within a dbt project. 
  -- Upon running dbt, this code will generate a new table called top_sales_category with the top sales category for products.
=======================================================================================================================
-- To calculate the 30-day and 60-day sales totals using the Order and Product tables in dbt, you can use a SQL query with a JOIN, date arithmetic, and aggregate functions. Here's an example of how you can achieve this:

-- models/sales_totals.sql

{{ config(
  materialized='table',
  tags=['example']
) }}

WITH thirty_day_sales AS (
  SELECT
    product_id,
    SUM(quantity * price) AS sales_30_days
  FROM
    orders o
    JOIN products p ON o.product_id = p.id
  WHERE
    o.order_date >= CURRENT_DATE - INTERVAL '30 days'
  GROUP BY
    product_id
),
sixty_day_sales AS (
  SELECT
    product_id,
    SUM(quantity * price) AS sales_60_days
  FROM
    orders o
    JOIN products p ON o.product_id = p.id
  WHERE
    o.order_date >= CURRENT_DATE - INTERVAL '60 days'
  GROUP BY
    product_id
)
SELECT
  p.id AS product_id,
  p.name AS product_name,
  COALESCE(thirty_day_sales.sales_30_days, 0) AS sales_30_days,
  COALESCE(sixty_day_sales.sales_60_days, 0) AS sales_60_days
FROM
  products p
  LEFT JOIN thirty_day_sales ON p.id = thirty_day_sales.product_id
  LEFT JOIN sixty_day_sales ON p.id = sixty_day_sales.product_id

-- In this example, we're creating two common table expressions (CTEs) to calculate the sales totals for the 30-day and 60-day periods. 
  -- We then perform a LEFT JOIN on the products table to ensure that all products are included in the final result, even if they have no sales data for the specified period. 
  -- The COALESCE function is used to handle cases where there are no sales for a product within the specified period.

-- This SQL code can be included in a file under the models directory within a dbt project. 
  --Upon running dbt, this code will generate a new table called sales_totals with the calculated sales totals for the 30-day and 60-day periods.

======================================================================================
-- To calculate the 30-day and 60-day sales totals grouped by Vendor ID using the Order and Product tables in dbt, 
  -- you can modify the previous example by adding the Vendor ID and adjusting the GROUP BY clause accordingly. Here's the modified SQL query:

-- models/sales_totals_grouped_by_vendor.sql

{{ config(
  materialized='table',
  tags=['example']
) }}

WITH thirty_day_sales AS (
  SELECT
    p.vendor_id,
    SUM(o.quantity * p.price) AS sales_30_days
  FROM
    orders o
    JOIN products p ON o.product_id = p.id
  WHERE
    o.order_date >= CURRENT_DATE - INTERVAL '30 days'
  GROUP BY
    p.vendor_id
),
sixty_day_sales AS (
  SELECT
    p.vendor_id,
    SUM(o.quantity * p.price) AS sales_60_days
  FROM
    orders o
    JOIN products p ON o.product_id = p.id
  WHERE
    o.order_date >= CURRENT_DATE - INTERVAL '60 days'
  GROUP BY
    p.vendor_id
)
SELECT
  p.vendor_id,
  COALESCE(thirty_day_sales.sales_30_days, 0) AS sales_30_days,
  COALESCE(sixty_day_sales.sales_60_days, 0) AS sales_60_days
FROM
  products p
  LEFT JOIN thirty_day_sales ON p.vendor_id = thirty_day_sales.vendor_id
  LEFT JOIN sixty_day_sales ON p.vendor_id = sixty_day_sales.vendor_id

-- In this modified example, we're grouping the sales totals by Vendor ID. We adjust the SELECT and GROUP BY clauses to include and group by the Vendor ID from the products table. 
-- The rest of the logic for calculating the 30-day and 60-day sales totals remains the same.
-- This SQL code can be included in a file under the models directory within a dbt project. 
-- Upon running dbt, this code will generate a new table called sales_totals_grouped_by_vendor with the grouped sales totals for the 30-day and 60-day periods.
