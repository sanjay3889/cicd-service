INSERT INTO `{prj_data_quality}.{dst_data_quality}.{tbl_dq_run_result}`
(dq_id, run_timestamp, attribute_arr, dq_score, invalid_rows, total_rows, invalid_value, total_value, param_date)
WITH params AS (
    SELECT cast(@DQ_ID AS STRING) AS dq_id,
           cast(@RUN_TIMESTAMP AS TIMESTAMP) AS run_timestamp,
           cast(@PARAM_DATE AS STRING) AS param_date
),

{% set parent_data = ({
    'vw_dim_product': ['{prj_spmena_apac}.{dst_cds_sellout_eu}.{vw_dim_product}', 'product_code', 'dim.{{source_column}}'],
    'vw_dim_point_of_sales': ['{prj_spmena_apac}.{dst_cds_sellout_eu}.{vw_dim_point_of_sales}', 'point_of_sale_code', 'dim.{{source_column}}'],
    'vw_fact_sellout': [null, null, 'fct.{{source_column}}']
})
%}

src_data AS (
    {% set parent_table = parent_data[source_table][0] %}
    {% set parent_col = parent_data[source_table][1] %}
    {% set parent_col2 = parent_data[source_table][2] %}
    
    SELECT fct.division_code
         , fct.market_code
         , CAST(EXTRACT(YEAR FROM sellout_date) AS STRING) AS sellout_year
         , fct.point_of_sale_code
         , {{parent_col2}} as source_column
         , {{val_column}} as val_column
         , {{source_table}} as source_table
      FROM `{prj_spmena_apac}.{dst_cds_sellout_eu}.{vw_fact_sellout}` AS fct
      {% if source_table != 'vw_fact_sellout' %}
      LEFT
      JOIN `{{ parent_table }}` AS dim
        ON fct.{{parent_col}} = dim.{{parent_col}}
      {% endif %}
),

src_data_pos_hierarchy as (
    SELECT src.division_code
         , src.market_code
         , src.sellout_year
         , pos.channel_level1
         , pos.channel_level2
         , pos.client_name
         , src.source_column
         , src.val_column
      FROM src_data as src
      LEFT
      JOIN `{prj_spmena_apac}.{dst_cds_sellout_eu}.{vw_dim_point_of_sales}` as pos
        ON src.point_of_sale_code = pos.point_of_sale_code
),

cols_calculated AS (
    SELECT division_code
         , market_code
         , sellout_year
         , channel_level1
         , channel_level2
         , client_name
         , SUM(CASE WHEN {{ condt }}
                    THEN val_column ELSE 0 END) AS invalid_value
         , SUM(val_column) AS total_value
         , COUNT(CASE WHEN {{ condt }}
                      THEN 1 ELSE NULL END) AS invalid_rows
         , COUNT(1) AS total_rows
    FROM src_data_pos_hierarchy
    GROUP BY all
),

final_cte as (
    SELECT dq_id
         , run_timestamp
         , [
              struct('division_code'  as key, division_code  as value),
              struct('market_code'    as key, market_code    as value),
              struct('sellout_year'   as key, sellout_year   as value),
              struct('channel_level1' as key, channel_level1 as value),
              struct('channel_level2' as key, channel_level2 as value),
              struct('client_name'    as key, client_name    as value)
           ] as key_value
         , cast(case when total_rows is not null and total_rows <> 0 
                     then (1 - coalesce(safe_divide(invalid_rows , total_rows), 1)) * 100 
                 end as numeric) as dq_score
         , invalid_rows
         , total_rows
         , invalid_value
         , total_value
         , param_date
      FROM params
      LEFT
      JOIN cols_calculated
        ON (1 = 1)
)

select * from final_cte;
