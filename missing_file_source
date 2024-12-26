with vw_fact_sellout as (
    SELECT DISTINCT 
           fct.market_code
         , fct.division_code
         , fct.source_project
         , fct.point_of_sale_code
         , fct.sellout_date
         , dim_pos.client_name
         , dim_pos.channel_level1
         , dim_pos.channel_level2
         , UPPER(dim_ds.data_source_frequency) as data_source_frequency
      FROM `spmena-onedata-dna-apac-pd.cds_spmena_sellout_eu.vw_fact_sellout`      as fct
      JOIN `spmena-onedata-dna-apac-pd.cds_spmena_sellout_eu.vw_dim_datasource`     as dim_ds
        ON fct.data_source = dim_ds.data_source_code
      JOIN `spmena-onedata-dna-apac-pd.cds_spmena_sellout_eu.vw_dim_point_of_sales` as dim_pos
        ON fct.point_of_sale_code = dim_pos.point_of_sale_code
     WHERE data_source_frequency is not null
       AND data_source_frequency <> '' 
       AND data_source_code <> '-1'
       AND last_day(fct.sellout_date) < CURRENT_DATE()
      --  and fct.sellout_date < CURRENT_DATE()
),

vw_dim_point_of_sales as (
    SELECT client_name
         , channel_level1
         , channel_level2
         , point_of_sale_code
      FROM `spmena-onedata-dna-apac-pd.cds_spmena_sellout_eu.vw_dim_point_of_sales`
),

vw_dim_calendar as (
    SELECT date(date_time) as cal_date
         , year
         , month
         , week_start_date
         , week_num
      FROM `spmena-onedata-dna-apac-pd.cds_spmena_sellout_eu.vw_dim_calendar`
     WHERE year >= 2021
       AND LAST_DAY(CAST(date_time AS date)) < CURRENT_DATE()
      --  and date(week_start_date) <= date_trunc(current_date, month)
),

active_stores as (
    SELECT distinct concat(fct.division_code, "_", fct.point_of_sale_code, "_", fct.client_name, "_", fct.source_project) as unq_col
      FROM vw_fact_sellout as fct
     WHERE sellout_date >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH), MONTH)
),

--calculating expected data
market_dtl AS (
    SELECT fct.market_code
         , fct.division_code
         , fct.point_of_sale_code
         , fct.client_name
         , fct.source_project
         , fct.channel_level1
         , fct.channel_level2
         , fct.data_source_frequency
         , oms_dt.store_created_date
         , CASE WHEN oms_dt.store_created_date IS NOT NULL and oms_dt.store_created_date >= date('2021-01-01')
                THEN DATE_TRUNC(DATE_ADD(oms_dt.store_created_date, INTERVAL 1 MONTH), MONTH)
                ELSE MIN(fct.sellout_date)
           END AS effective_store_start_date
      FROM vw_fact_sellout as fct
      LEFT
      JOIN `apmena-onedata-dna-apac-pd.mds_stg_outbound_eu.oms_store_start_dates` as oms_dt
        ON concat(market, "_", platform, "_", oms_store_code) = fct.point_of_sale_code
     WHERE concat(fct.division_code, "_", fct.point_of_sale_code, "_", client_name, "_", fct.source_project) 
            in (select * from active_stores)
     GROUP BY ALL
),

expected_daily_data AS (
    SELECT year
         , month
         , market_code
         , division_code
         , point_of_sale_code
         , client_name
         , source_project
         , channel_level1
         , channel_level2
         , data_source_frequency
         , ARRAY_AGG(extract(day from cal_date) order by extract(day from cal_date)) as expected_data
      FROM vw_dim_calendar as dim_cal
     CROSS
      JOIN market_dtl   as mrk     
     WHERE dim_cal.cal_date >= mrk.effective_store_start_date
       AND data_source_frequency = 'DAILY'
      --  and last_day(cal_date) < current_date()
     GROUP BY ALL
),

expected_weekly_data as (
    SELECT extract(year from week_start_date) as year
         , extract(month from week_start_date) as month
         , market_code
         , division_code
         , point_of_sale_code
         , client_name
         , source_project
         , channel_level1
         , channel_level2
         , data_source_frequency
         , ARRAY_AGG(distinct week_num order by week_num) as expected_data
      FROM vw_dim_calendar as dim_cal
     CROSS 
      JOIN market_dtl as mrk
     WHERE dim_cal.cal_date >= mrk.effective_store_start_date
       AND cal_date between date_trunc(date('2021-01-01'), week(monday)) + 7
                        and date_trunc(date_trunc(current_date(), month), week(sunday))
               -- it will consider the last sunday of the previous month
                    --     and case when format_date('%A',  date_trunc(current_date(), month)-1) <> 'Sunday'
                    --              then date_trunc(date_trunc(current_date(), month), week(sunday)) - 7
                    --              else date_trunc(date_trunc(current_date(), month), week(sunday))
                    --              end
       AND data_source_frequency = 'WEEKLY'
     GROUP BY ALL     
),

expected_monthly_data AS (
    SELECT DISTINCT year
         , month
         , market_code
         , division_code
         , point_of_sale_code
         , client_name
         , source_project
         , channel_level1
         , channel_level2
         , data_source_frequency
         , [1] as expected_data
      FROM vw_dim_calendar as dim_cal
     CROSS
      JOIN market_dtl as mrk
     WHERE dim_cal.cal_date >= mrk.effective_store_start_date
       AND data_source_frequency = 'MONTHLY'
),

--bringing sales/qty = 0 data from sds

ECOMM_SKU_TRAFFIC_SALES_ZERO as (     
    with src as (
        SELECT market_code
             , case when division='ACD' then 'LDB' else division end as division_code
             , date_id
             , case when (ifnull(market_code, '') = '' or ifnull(client, '') = '') then '-1'
                    else concat(market_code,'_',client,'_',coalesce(platform_store_id, 'NA'))
                end as point_of_sale_code
             , case when  (market_code is null or division = '-1' or client is null or period is null ) then '-1'
                    else concat(market_code, '_',division, '_', client, '_', 
                                 case when period= 'D' THEN 'DAY' END , '_', 'SO') 
                end as data_source
             , 'ECOMM_SKU_TRAFFIC' as source_project
             , gross_sales
             , gross_sold_units
             , gross_orders
          FROM `spmena-onedataraw-dna-apac-pd.warehouse_ecommerce_eu.fact_ecomm_sku_traffic_sellout`
         WHERE market_code not in ('BR','TW')
           AND extract(year from date_id) >= 2021
           AND period = 'D'
           AND upper(file_type) = 'SKU TRAFFIC'
           AND upper(coalesce(sap_signature_code,'-1')) not in('PH')
    )
    
        SELECT fct.market_code
             , fct.division_code
             , source_project
             , fct.point_of_sale_code
             , date_id as sellout_date
             , client_name
             , channel_level1
             , channel_level2
             , sum(gross_sales) as sales
             , sum(gross_sold_units) as units
             , sum(gross_orders) as orders
          FROM src as fct
          JOIN vw_dim_point_of_sales as dim_pos
            ON fct.point_of_sale_code = dim_pos.point_of_sale_code
          WHERE concat(fct.division_code, "_", fct.point_of_sale_code, "_", client_name, "_", fct.source_project) 
             in (select * from active_stores)
          GROUP BY all
         HAVING sales = 0 or units = 0 or orders = 0
),

--calculating available data

available_daily_data AS (
    SELECT fct.market_code
         , fct.division_code
         , fct.point_of_sale_code
         , EXTRACT(year FROM  fct.sellout_date) AS year
         , EXTRACT(month FROM fct.sellout_date) AS month
         , client_name
         , source_project
         , channel_level1
         , channel_level2
         , array_agg(distinct extract(day from sellout_date) order by extract(day from sellout_date)) as available_data
      FROM ((SELECT * except(data_source_frequency) FROM vw_fact_sellout WHERE data_source_frequency = 'DAILY') 
             UNION ALL
             SELECT * except(sales, units, orders) FROM ECOMM_SKU_TRAFFIC_SALES_ZERO)as fct
     GROUP BY ALL
),

available_weekly_data as (
    SELECT fct.market_code
         , fct.division_code
         , fct.point_of_sale_code
         , extract(year from week_start_date) as year 
         , extract(month from week_start_date) as month
         , fct.client_name
         , fct.source_project
         , fct.channel_level1
         , fct.channel_level2
         , array_agg(distinct week_num order by week_num) as available_data
      FROM vw_fact_sellout as fct
      JOIN vw_dim_calendar       as dim_cal
        ON fct.sellout_date = dim_cal.cal_date
     WHERE sellout_date between date_trunc(date('2021-01-01'), week(Monday))+ 7
                            and date_trunc(date_trunc(current_date(), month), week(sunday))
       AND data_source_frequency = 'WEEKLY'
     GROUP BY ALL
),

available_monthly_data AS (
    SELECT DISTINCT fct.market_code
         , fct.division_code
         , fct.point_of_sale_code
         , EXTRACT(year FROM  fct.sellout_date) AS year
         , EXTRACT(month FROM fct.sellout_date) AS month
         , client_name
         , fct.source_project
         , channel_level1
         , channel_level2
         , [1] as available_data
      FROM vw_fact_sellout as fct
     WHERE data_source_frequency = 'MONTHLY'
),

--calculating missing data

missing_data as (
    SELECT exp.market_code
         , exp.division_code
         , exp.point_of_sale_code
         , exp.client_name
         , exp.source_project
         , exp.year
         , exp.month
         , exp.channel_level1
         , exp.channel_level2
         , exp.data_source_frequency
         , array_length(array( select 1 from unnest(exp.expected_data) a 
                               where a not in (select b from unnest(fct.available_data) b) )) missing_file_count
      FROM (select * from expected_daily_data
            union all
            select * from expected_weekly_data
            union all
            select * from expected_monthly_data) as exp
      LEFT
      JOIN (select * from available_daily_data
            union all
            select * from available_weekly_data
            union all
            select * from available_monthly_data) as fct
     USING (division_code, point_of_sale_code, year, month, source_project, client_name)
     GROUP BY ALL
),

total_missing_data as (
  SELECT market_code
         , division_code
         , point_of_sale_code
         , client_name
         , source_project
         , year
         , month
         , channel_level1
         , channel_level2
         , data_source_frequency
         , missing_file_count
         , case when missing_file_count > 0 then 1 else 0 end as missing_file
      FROM missing_data
),

before_store_start_date as (
  SELECT DISTINCT market_code
         , division_code
         , point_of_sale_code
         , client_name
         , source_project
         , year
         , month
         , channel_level1
         , channel_level2
         , data_source_frequency
         , null as missing_file_count
         , 0 as missing_file
      FROM vw_dim_calendar as dim_cal
     CROSS
      JOIN market_dtl   as mrk     
     WHERE case when data_source_frequency in ('DAILY', 'MONTHLY')
                then cal_date < DATE_TRUNC(effective_store_start_date, month)
                else cal_date < DATE_TRUNC(DATE_TRUNC(effective_store_start_date, week(monday)), MONTH)
            end
),

final as (
SELECT * FROM total_missing_data
UNION ALL
SELECT * FROM before_store_start_date
)

SELECT * FROM final
