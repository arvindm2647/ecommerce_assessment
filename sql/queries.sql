-- ============================================
-- SQL Queries for E-commerce Analytics
-- ============================================

-- Query 1: Daily Revenue (Paid Orders)
-- Total revenue grouped by day
SELECT 
    DATE(order_date) AS order_day,
    SUM(total_amount) AS daily_revenue,
    COUNT(order_id) AS total_orders
FROM orders
WHERE status = 'paid'
GROUP BY DATE(order_date)
ORDER BY order_day DESC;


-- Query 2: Top 5 Products by Revenue (Last 7 Days)
-- Return product ID, product name, total units, and total revenue
SELECT 
    p.product_id,
    p.product_name,
    SUM(oi.quantity) AS total_units_sold,
    SUM(oi.quantity * oi.unit_price) AS total_revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
JOIN orders o ON oi.order_id = o.order_id
WHERE o.status = 'paid'
    AND o.order_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY p.product_id, p.product_name
ORDER BY total_revenue DESC
LIMIT 5;


-- Query 3: User Retention (D+7)
-- For each signup date, calculate the percentage of users who return 7 days later
WITH user_signups AS (
    SELECT 
        DATE(signup_date) AS signup_day,
        user_id
    FROM users
),
day7_activity AS (
    SELECT 
        us.signup_day,
        us.user_id,
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM events e 
                WHERE e.user_id = us.user_id 
                AND DATE(e.event_timestamp) = us.signup_day + INTERVAL '7 days'
            ) THEN 1 
            ELSE 0 
        END AS returned_d7
    FROM user_signups us
)
SELECT 
    signup_day,
    COUNT(user_id) AS total_signups,
    SUM(returned_d7) AS returned_users,
    ROUND(100.0 * SUM(returned_d7) / COUNT(user_id), 2) AS retention_percentage
FROM day7_activity
GROUP BY signup_day
ORDER BY signup_day;


-- Query 4: Order Funnel
-- From events, calculate counts and conversion rates through stages: page_view → add_to_cart → purchase
WITH funnel_counts AS (
    SELECT 
        COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) AS page_views,
        COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS add_to_carts,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS purchases
    FROM events
)
SELECT 
    'page_view' AS stage,
    page_views AS count,
    100.0 AS conversion_rate_from_previous,
    ROUND(100.0 * page_views / page_views, 2) AS conversion_rate_from_start
FROM funnel_counts

UNION ALL

SELECT 
    'add_to_cart' AS stage,
    add_to_carts AS count,
    ROUND(100.0 * add_to_carts / NULLIF(page_views, 0), 2) AS conversion_rate_from_previous,
    ROUND(100.0 * add_to_carts / NULLIF(page_views, 0), 2) AS conversion_rate_from_start
FROM funnel_counts

UNION ALL

SELECT 
    'purchase' AS stage,
    purchases AS count,
    ROUND(100.0 * purchases / NULLIF(add_to_carts, 0), 2) AS conversion_rate_from_previous,
    ROUND(100.0 * purchases / NULLIF(page_views, 0), 2) AS conversion_rate_from_start
FROM funnel_counts;

