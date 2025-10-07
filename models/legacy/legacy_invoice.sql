-- CTE-less messy SQL for dbt practice (fixed)
-- Generates invoices + product metrics
-- Sources: companies_base, employees_base, products_base, enterprise_orders_base, fake_personal_info

SELECT * 
FROM (
    -- Invoices part
    SELECT
        'invoice' AS record_type,
        inv.invoice_id,
        c.id AS comp_id,
        c.name AS comp_name,
        e.id AS emp_id,
        e.first_name,
        e.last_name,
        e.email,
        pinfo.phone_number,
        pinfo.address,
        NULL AS prod_id,
        NULL AS prod_name,
        NULL AS category,
        inv.lines_count,
        round(cast(inv.invoice_gross as numeric),2,'ROUND_HALF_AWAY_FROM_ZERO') as invoice_gross,
        round(cast(inv.invoice_net_before_vat as numeric),2,'ROUND_HALF_AWAY_FROM_ZERO') as invoice_net_before_vat,
        round(cast(inv.invoice_vat_total as numeric),2,'ROUND_HALF_AWAY_FROM_ZERO') as invoice_vat_total,
        round(cast(inv.invoice_total as numeric),2,'ROUND_HALF_AWAY_FROM_ZERO') as invoice_total,
        inv.invoice_value_bucket,
        CURRENT_TIMESTAMP() AS generated_at
    FROM (
        -- invoice totals calculation
        SELECT
            invoice_id,
            emp_id,
            COUNT(*) AS lines_count,
            SUM(gross_amount) AS invoice_gross,
            SUM(net_after_discount) AS invoice_net_before_vat,
            SUM(vat_amount) AS invoice_vat_total,
            SUM(final_line_amount) AS invoice_total,
            CASE
              WHEN SUM(final_line_amount) > 1000 THEN 'high_value'
              WHEN SUM(final_line_amount) BETWEEN 500 AND 1000 THEN 'mid_value'
              ELSE 'low_value'
            END AS invoice_value_bucket
        FROM (
            -- invoice lines
            SELECT
                o.emp_id,
                concat(order_date, emp_id) AS invoice_id,
                o.num_items * p.price AS gross_amount,
                (o.num_items * p.price) * (1 - COALESCE(GREATEST(
                      CASE
                        WHEN emp_orders.orders_count >= 100 THEN 0.05
                        WHEN emp_orders.orders_count >= 50 THEN 0.03
                        WHEN emp_orders.orders_count >= 10 THEN 0.01
                        ELSE 0.00
                      END,
                      CASE
                        WHEN DATE_DIFF(CURRENT_DATE(), c.date_added, YEAR) >= 5 THEN 0.10
                        WHEN DATE_DIFF(CURRENT_DATE(), c.date_added, YEAR) >= 2 THEN 0.05
                        WHEN comp_orders.company_orders > 200 THEN 0.04
                        ELSE 0.00
                      END
                ),0)) AS net_after_discount,
                (o.num_items * p.price) * (
                  CASE
                    WHEN LOWER(p.category) LIKE '%food%' THEN 0.05
                    WHEN LOWER(p.category) LIKE '%electronics%' THEN 0.20
                    WHEN LOWER(p.category) LIKE '%books%' THEN 0.00
                    WHEN LOWER(p.category) LIKE '%clothing%' THEN 0.12
                    ELSE 0.18
                  END
                ) AS vat_amount,
                (o.num_items * p.price) * (1 - COALESCE(GREATEST(
                      CASE
                        WHEN emp_orders.orders_count >= 100 THEN 0.05
                        WHEN emp_orders.orders_count >= 50 THEN 0.03
                        WHEN emp_orders.orders_count >= 10 THEN 0.01
                        ELSE 0.00
                      END,
                      CASE
                        WHEN DATE_DIFF(CURRENT_DATE(), c.date_added, YEAR) >= 5 THEN 0.10
                        WHEN DATE_DIFF(CURRENT_DATE(), c.date_added, YEAR) >= 2 THEN 0.05
                        WHEN comp_orders.company_orders > 200 THEN 0.04
                        ELSE 0.00
                      END
                ),0))
                + (o.num_items * p.price) * (
                  CASE
                    WHEN LOWER(p.category) LIKE '%food%' THEN 0.05
                    WHEN LOWER(p.category) LIKE '%electronics%' THEN 0.20
                    WHEN LOWER(p.category) LIKE '%books%' THEN 0.00
                    WHEN LOWER(p.category) LIKE '%clothing%' THEN 0.12
                    ELSE 0.18
                  END
                ) AS final_line_amount,
                o.order_date
            FROM (
                -- enterprise_orders_base with fake order_date
                SELECT 
                  employee_id AS emp_id,
                  product_id,
                  num_items,
                  date AS order_date
                FROM `dbt-fake-1`.`dbt_aleonel_sources`.`enterprise_orders_base`
            ) o
            LEFT JOIN `dbt-fake-1`.`dbt_aleonel_sources`.`products_base` p
              ON o.product_id = p.id
            LEFT JOIN `dbt-fake-1`.`dbt_aleonel_sources`.`employees_base` e
              ON o.emp_id = e.id
            LEFT JOIN `dbt-fake-1`.`dbt_aleonel_sources`.`companies_base` c
              ON e.company_id = c.id
            LEFT JOIN (
              SELECT employee_id, COUNT(*) AS orders_count
              FROM `dbt-fake-1`.`dbt_aleonel_sources`.`enterprise_orders_base`
              GROUP BY employee_id
            ) emp_orders
              ON o.emp_id = emp_orders.employee_id
            LEFT JOIN (
              SELECT e.company_id AS comp_id, COUNT(*) AS company_orders
              FROM `dbt-fake-1`.`dbt_aleonel_sources`.`enterprise_orders_base` eo
              JOIN `dbt-fake-1`.`dbt_aleonel_sources`.`employees_base` e
                ON eo.employee_id = e.id
              GROUP BY comp_id
            ) comp_orders
              ON e.company_id = comp_orders.comp_id
        ) invoice_lines
        GROUP BY invoice_id, emp_id
    ) inv
    LEFT JOIN `dbt-fake-1`.`dbt_aleonel_sources`.`employees_base` e
      ON inv.emp_id = e.id
    LEFT JOIN `dbt-fake-1`.`dbt_aleonel_sources`.`companies_base` c
      ON e.company_id = c.id
    LEFT JOIN `dbt-fake-1`.`dbt_aleonel_source_seeds`.`fake_personal_info` pinfo
      ON e.id = pinfo.id ) final_output
ORDER BY record_type, invoice_total DESC NULLS LAST, generated_at DESC
