-- Повторно очищуємо цільову таблицю Bronze
TRUNCATE TABLE bronze.crm_sales_details;

-- Вставляємо дані, додаючи захист від неформатованих чисел
INSERT INTO bronze.crm_sales_details (
    sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
)
SELECT
    s_col_1, 
    s_col_2, 
    NULLIF(s_col_3, '')::INT, 

    -- ДАТИ: Обробляємо "0" ТА невалідні числові рядки (< 8 символів)
    -- Якщо рядок = '0' АБО довжина не 8, перетворюємо його на NULL. Потім приводимо до DATE.
    NULLIF(
        NULLIF(s_col_4, '0'),  -- Спочатку видаляємо '0'
        CASE WHEN LENGTH(s_col_4) <> 8 THEN s_col_4 ELSE NULL END -- Потім видаляємо все, що не 8 символів
    )::DATE AS sls_order_dt,
    
    NULLIF(
        NULLIF(s_col_5, '0'),
        CASE WHEN LENGTH(s_col_5) <> 8 THEN s_col_5 ELSE NULL END
    )::DATE AS sls_ship_dt,
    
    NULLIF(
        NULLIF(s_col_6, '0'),
        CASE WHEN LENGTH(s_col_6) <> 8 THEN s_col_6 ELSE NULL END
    )::DATE AS sls_due_dt,

    -- Числа: Логіка Чисел залишається правильною
    COALESCE(NULLIF(s_col_7, '')::NUMERIC(10, 2), 0.00), 
    COALESCE(NULLIF(s_col_8, '')::INT, 0), 
    COALESCE(NULLIF(s_col_9, '')::NUMERIC(10, 2), 0.00) 
FROM
    bronze.crm_sales_details_stage;
