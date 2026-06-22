-- Databricks Sales Lakehouse - Dashboard Queries

USE sales_lakehouse;

-- 1. Faturamento diário
SELECT
    order_date,
    orders_count,
    items_sold,
    gross_revenue,
    avg_ticket
FROM gold_daily_sales
ORDER BY order_date;

-- 2. Vendas por categoria
SELECT
    category,
    orders_count,
    items_sold,
    gross_revenue,
    avg_ticket
FROM gold_sales_by_category
ORDER BY gross_revenue DESC;

-- 3. Top 10 clientes por gasto
SELECT
    customer_id,
    customer_name,
    country,
    orders_count,
    items_bought,
    total_spent,
    last_order_date
FROM gold_top_customers
ORDER BY total_spent DESC
LIMIT 10;

-- 4. Resumo de métodos de pagamento
SELECT
    payment_method,
    payment_status,
    payments_count,
    payment_amount_total,
    avg_payment_amount
FROM gold_payment_summary
ORDER BY payment_method, payment_status;

-- 5. Pedidos por status
SELECT
    order_status,
    orders_count,
    order_amount_total
FROM gold_order_status_summary
ORDER BY orders_count DESC;

-- 6. Checks de qualidade
SELECT
    check_name,
    table_name,
    status,
    observed_value,
    expected_rule,
    checked_at
FROM dq_results
ORDER BY status, table_name, check_name;
