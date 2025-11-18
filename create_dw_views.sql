-- ============================================
-- View 1: Orders grouped by delivery status
-- ============================================
CREATE OR REPLACE VIEW dw.v_delivery_status AS
SELECT
    CASE
        WHEN delivery_delay_days > 0 THEN 'Late'
        WHEN delivery_delay_days = 0 THEN 'On time'
        WHEN delivery_delay_days < 0 THEN 'Early'
        ELSE 'Unknown'
    END AS delivery_status,
    COUNT(*) AS num_orders,
    ROUND(AVG(delivery_delay_days)::NUMERIC, 2) AS avg_delay_days
FROM dw.fact_orders
GROUP BY 1;

-- ============================================
-- View 2: Average delivery delay by state
-- ============================================
CREATE OR REPLACE VIEW dw.v_avg_delay_by_state AS
SELECT
    dc.customer_state AS state,
    ROUND(AVG(fo.delivery_delay_days)::NUMERIC, 2) AS avg_delay_days,
    COUNT(*) AS total_orders
FROM dw.fact_orders fo
JOIN dw.dim_customers dc
    ON fo.customer_sk = dc.customer_sk
GROUP BY dc.customer_state
ORDER BY avg_delay_days DESC;

