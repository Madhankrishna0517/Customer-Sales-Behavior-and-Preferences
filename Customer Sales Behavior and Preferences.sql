/* --------------------
   Case Study Questions
   --------------------*/

-- 1. What is the total amount each customer spent at the restaurant?

Select Distinct sales.customer_id,sum(menu.price) as total_amt_spent from dd.sales as sales
inner join dd.menu as menu on sales.product_id=menu.product_id
group by sales.customer_id


-- 2. How many days has each customer visited the restaurant?
SELECT 
    customer_id,
    COUNT(DISTINCT order_date) AS days_visited
FROM 
    dd.sales
GROUP BY 
    customer_id;

-- 3. What was the first item from the menu purchased by each customer?
SELECT sub.customer_id, sub.product_name
FROM (
    SELECT s.customer_id, m.product_name,
           ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.order_date ASC) AS rnk
    FROM dd.menu AS m
    JOIN dd.sales AS s ON m.product_id = s.product_id
) AS sub
WHERE sub.rnk = 1;

-- 4. What is the most purchased item on the menu and how many times was it purchased by all customers?

Select m.product_name as product,COUNT(s.customer_id) as no_of_times from  dd.sales as s
join dd.menu as m  on s.product_id=m.product_id
group by m.product_name
order by no_of_times desc
limit  1

-- 5. Which item was the most popular for each customer?
WITH RankedProducts AS (
    SELECT 
        s.customer_id,
        m.product_name,
        COUNT(*) AS order_count,
        RANK() OVER (PARTITION BY s.customer_id ORDER BY COUNT(*) DESC) AS rnk
    FROM 
        dd.sales s
    JOIN 
        dd.menu m ON s.product_id = m.product_id
    GROUP BY 
        s.customer_id, m.product_name
)
SELECT 
    customer_id,
    product_name,
    order_count
FROM 
    RankedProducts
WHERE 
    rnk = 1;



-- 6. Which item was purchased first by the customer after they became a member?

   WITH joined_as_member AS (
  SELECT
    members.customer_id, 
    sales.product_id,
    ROW_NUMBER() OVER (
      PARTITION BY members.customer_id
      ORDER BY sales.order_date) AS row_num
  FROM dd.members
  INNER JOIN dd.sales
    ON members.customer_id = sales.customer_id
    AND sales.order_date > members.join_date
)

SELECT 
  customer_id, 
  product_name 
FROM joined_as_member
INNER JOIN dd.menu
  ON joined_as_member.product_id = menu.product_id
WHERE row_num = 1
ORDER BY customer_id ASC;


-- 7. Which item was purchased just before the customer became a member?

  WITH before_joined_as_member AS (
  SELECT
    members.customer_id, 
    sales.product_id,
    ROW_NUMBER() OVER (
      PARTITION BY members.customer_id
      ORDER BY sales.order_date desc) AS row_num
  FROM dd.members
  INNER JOIN dd.sales
    ON members.customer_id = sales.customer_id
    AND sales.order_date < members.join_date
)

SELECT 
  customer_id, 
  product_name 
FROM before_joined_as_member
INNER JOIN dd.menu
  ON before_joined_as_member.product_id = menu.product_id
WHERE row_num = 1
ORDER BY customer_id ASC;


-- 8. What is the total items and amount spent for each member before they became a member?

select s.customer_id,count(s.*)as total_items,sum(mn.price) as amt_spent from dd.menu mn
join dd.sales s on mn.product_id=s.product_id
join dd.members m on s.customer_id=m.customer_id
where m.join_date>s.order_date
group by s.customer_id
order by amt_spent desc 


-- 9.  If each $1 spent equates to 10 points and sushi has a 2x points multiplier - how many points would each customer have?

with prod_points as (
select mn.product_id,
case
	when mn.product_id=1 then 20*mn.price
	else 10*mn.price 
end as total_points
from dd.menu as mn
)

select s.customer_id,sum(total_points) from prod_points as pd
join dd.sales s on pd.product_id=s.product_id 
group by s.customer_id 


-- 10. In the first week after a customer joins the program (including their join date) they earn 2x points on all items, not just sushi - how many points do customer A and B have at the end of January?

WITH dates_cte AS (
  SELECT 
    customer_id, 
      join_date, 
      join_date + 6 AS valid_date, 
      DATE_TRUNC(
        'month', '2021-01-31'::DATE)
        + interval '1 month' 
        - interval '1 day' AS last_date
  FROM dd.members
)

SELECT 
  sales.customer_id, 
  SUM(CASE
    WHEN menu.product_name = 'sushi' THEN 2 * 10 * menu.price
    WHEN sales.order_date BETWEEN dates.join_date AND dates.valid_date THEN 2 * 10 * menu.price
    ELSE 10 * menu.price END) AS points
FROM dd.sales
INNER JOIN dates_cte AS dates
  ON sales.customer_id = dates.customer_id
  AND dates.join_date <= sales.order_date
  AND sales.order_date <= dates.last_date
INNER JOIN dd.menu
  ON sales.product_id = menu.product_id
GROUP BY sales.customer_id;
