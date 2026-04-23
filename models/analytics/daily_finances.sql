WITH daily_revenue AS (
    SELECT
        DATE(order_at)          AS finance_date,
        COUNT(DISTINCT order_id) AS order_count,
        SUM(gross_revenue)      AS gross_revenue
    FROM {{ ref('fact_order') }}
    WHERE is_returned = FALSE
    GROUP BY DATE(order_at)
)

SELECT
    COALESCE(r.finance_date, c.finance_date)            AS finance_date,
    COALESCE(r.order_count, 0)                          AS order_count,
    COALESCE(r.gross_revenue, 0)                        AS gross_revenue,
    COALESCE(c.total_cost, 0)                           AS total_cost,
    COALESCE(r.gross_revenue, 0) - COALESCE(c.total_cost, 0) AS profit
FROM daily_revenue AS r
FULL OUTER JOIN {{ ref('int_daily_costs') }} AS c
    ON r.finance_date = c.finance_date