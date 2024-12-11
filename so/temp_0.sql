INSERT INTO `{prj_data_quality}.{dst_data_quality}.{tbl_dq_run_result}`
(dq_id, run_timestamp, attribute_arr, dq_score, invalid_rows, total_rows, invalid_value, total_value, param_date)
WITH params AS (
    SELECT cast(@DQ_ID AS STRING) AS dq_id,
           cast(@RUN_TIMESTAMP AS TIMESTAMP) AS run_timestamp,
           cast(@PARAM_DATE AS STRING) AS param_date
),
{% set parent_data = ({
    'PRODUCT.product_code': ['{prj_spmena_apac}.{dst_cds_sellout_eu}.{vw_dim_product}', 'product_code'],
    'POINT_OF_SALE.point_of_sale_code': ['{prj_spmena_apac}.{dst_cds_sellout_eu}.{vw_dim_point_of_sales}', 'point_of_sale_code'],
    'DATA_SOURCE.data_source_code': ['{prj_spmena_apac}.{dst_cds_sellout_eu}.{vw_dim_datasource}', 'data_source_code']
})
%}

src_data AS (
    {% set parent_table = parent_data[include_column][0] %}
    {% set parent_column = parent_data[include_column][1] %}
    SELECT fct.division_code,
           fct.market_code,
           CAST(EXTRACT(YEAR FROM sellout_date) AS STRING) AS sellout_year,
           fct.point_of_sale_code,
           fct.product_code,
           COALESCE(ABS(consolidated_sales_eur), 0) AS soval,
           COUNT(*) OVER (PARTITION BY fct.{{ include_column }}) AS cnt,
           fct.{{ include_column }} AS source_column,
           dim.{{ parent_column }} AS parent_column
    FROM `{prj_spmena_apac}.{dst_cds_sellout_eu}.{vw_fact_sellout}` AS fct
    LEFT JOIN (
        SELECT DISTINCT {{ parent_column }}
        FROM `{{ parent_table }}`
    ) AS dim
    ON fct.{{ include_column }} = dim.{{ parent_column }}
),
cols_calculated AS (
    SELECT division_code,
           market_code,
           sellout_year,
           SUM(CASE WHEN parent_column IS NULL OR source_column IS NULL OR source_column = '' THEN soval ELSE 0 END) AS invalid_value,
           SUM(soval) AS total_value,
           COUNT(CASE WHEN parent_column IS NULL OR source_column IS NULL OR source_column = '' THEN 1 ELSE NULL END) AS invalid_rows,
           COUNT(1) AS total_rows
    FROM src_data
    GROUP BY 1, 2, 3
),
final_cte AS (
    SELECT dq_id,
           run_timestamp,
           [
               STRUCT('division_code' AS key, division_code AS value),
               STRUCT('market_code' AS key, market_code AS value),
               STRUCT('sellout_year' AS key, sellout_year AS value)
           ] AS key_value,
           CAST(CASE WHEN total_rows IS NOT NULL AND total_rows <> 0
                     THEN (1 - (invalid_rows / total_rows)) * 100
                END AS NUMERIC) AS dq_score,
           invalid_rows,
           total_rows,
           invalid_value,
           total_value,
           param_date
    FROM params
    CROSS JOIN cols_calculated
)
SELECT * FROM final_cte;
