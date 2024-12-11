INSERT INTO `{prj_data_quality}.{dst_data_quality}.{tbl_dq_run_result}` 
(dq_id, run_timestamp, attribute_arr, dq_score, invalid_rows, total_rows, invalid_value, total_value, param_date)
with params as (
    select cast(@DQ_ID         as STRING)    as dq_id
         , cast(@RUN_TIMESTAMP as TIMESTAMP) as run_timestamp
         , cast(@PARAM_DATE    as STRING)    as param_date
),
src_data as (
    SELECT fct.division_code
         , fct.market_code
         , CAST(EXTRACT(YEAR FROM sellout_date) AS STRING)            AS sellout_year
         , fct.point_of_sale_code
         , fct.product_code
         , dim.cnt
         , coalesce(abs(consolidated_sales_eur), 0)                            as soval
      from `{prj_spmena_apac}.{dst_cds_sellout_eu}.{vw_fact_sellout}`      as fct
      left
      join (select product_code
                 , count(*) over(partition by product_code)     as cnt
              from `{prj_spmena_apac}.{dst_cds_sellout_eu}.{vw_dim_product}`)  as dim
        on (fct.product_code = dim.product_code)
),
src_data_pos_hierarchy as (
    select src.division_code
         , src.market_code
         , src.sellout_year
         , pos.channel_level1
         , pos.channel_level2
         , pos.client_name
         , src.cnt
         , src.soval
      from src_data as src
      join `{prj_spmena_apac}.{dst_cds_sellout_eu}.{vw_dim_point_of_sales}` as pos
        on src.point_of_sale_code = pos.point_of_sale_code
),
cols_calculated as (
    select division_code
         , market_code
         , sellout_year
         , channel_level1
         , channel_level2
         , client_name
         , sum(case when cnt > 1 then soval else 0 end) as invalid_value
         , sum(soval) as total_value 
         , count(case when cnt > 1 then 1 else null end) as invalid_rows
         , count(1) as total_rows
      from src_data_pos_hierarchy
     group by 1, 2, 3, 4, 5, 6
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
                     then (1 - (invalid_rows / total_rows)) * 100
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
select * from final_cte
;
