BEGIN
-- EXECUTE IMMEDIATE 
create or replace table `oa-apmena-itdelivery-sp-np.Test_dataset.vw_dynamic_result`
partition by range_bucket(year, generate_array(2021,2025))
cluster by market_code, division_code, point_of_sale_code, month
 AS
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
         , dim_ds.data_source_frequency
      FROM --`oa-apmena-itdelivery-sp-np.Test_dataset.fact_sellout` as fct
       `spmena-onedata-dna-apac-pd.cds_spmena_sellout_eu.vw_fact_sellout`      as fct
      JOIN `spmena-onedata-dna-apac-pd.cds_spmena_sellout_eu.vw_dim_datasource`     as dim_ds
        ON fct.data_source = dim_ds.data_source_code
      JOIN `spmena-onedata-dna-apac-pd.cds_spmena_sellout_eu.vw_dim_point_of_sales` as dim_pos
        ON fct.point_of_sale_code = dim_pos.point_of_sale_code
     WHERE EXTRACT(year FROM  fct.sellout_date) >= 2021
       AND data_source_frequency is not null
       AND data_source_frequency <> '' 
       AND data_source_code <> '-1'
),

vw_dim_point_of_sales as (
    SELECT DISTINCT client_name
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
         , CASE WHEN fct.source_project = 'OMS_SFCC' AND oms_dt.store_created_date IS NOT NULL
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
         , ARRAY_AGG(extract(day from cal_date) order by extract(day from cal_date)) as expected_array
      FROM vw_dim_calendar as dim_cal
     CROSS
      JOIN market_dtl   as mrk     
     WHERE dim_cal.cal_date >= mrk.effective_store_start_date
       AND data_source_frequency = 'Daily'
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
         , ARRAY_AGG(distinct week_num order by week_num) as expected_array
      FROM vw_dim_calendar as dim_cal
     CROSS 
      JOIN market_dtl as mrk
     WHERE dim_cal.cal_date >= mrk.effective_store_start_date
       AND cal_date between date_trunc(date('2021-01-01'), week(monday)) + 7
                        and date_trunc(date_trunc(current_date(), month), week(sunday))
       AND data_source_frequency = 'Weekly'
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
      FROM vw_dim_calendar as dim_cal
     CROSS
      JOIN market_dtl as mrk
     WHERE dim_cal.cal_date >= mrk.effective_store_start_date
       AND data_source_frequency = 'Monthly'
),

--bringing sales/qty = 0 data from sds

ecom_amz_qty_zero as (
    WITH src as (
        SELECT coalesce(ecomm.market_code,'-1') AS market_code
             , coalesce(product_elix.division_code, product_ecc.division_code, dim_grp.division, ecomm.sap_division_name, '-1') AS division_code
             , date_id
             , 'ECOM_AMZ' AS source_project
             , client
             , CASE WHEN ecomm.period = 'D' THEN 'DAY' END AS period
             , channel_type
             , shipped_units 
             , shipped_revenue
             , ordered_units
             , ordered_revenue
          FROM `spmena-onedataraw-dna-apac-pd.warehouse_ecommerce_eu.fact_ecomm_amz` as ecomm
          LEFT
          JOIN `spmena-onedataraw-dna-apac-pd.sds_spmena_product_eu.dim_prioritized_ean_sku_mapping` AS priority_ean_sku
            ON TRIM(LTRIM(CASE WHEN TRIM(ecomm.ean_code) IN ('0','--','','-','(blank)','""') THEN NULL
                               WHEN REGEXP_CONTAINS(ecomm.ean_code,r'E\+') THEN TRIM(CAST(CAST(ecomm.ean_code AS NUMERIC) AS STRING))
                               ELSE TRIM(REPLACE(ecomm.ean_code, '┬á', ''))
                          END, '0')) = TRIM(LTRIM(priority_ean_sku.ean_code, '0'))
           AND ecomm.market_code = priority_ean_sku.src_market_code
           AND priority_ean_sku.prioritized_market_code = priority_ean_sku.market_code
           AND priority_ean_sku.ean_code <> '0000000000000'
           AND TRIM(priority_ean_sku.ean_code) <> ''
          LEFT
          JOIN (select * from `spmena-onedata-dna-apac-pd.cds_spmena_sellout_eu.vw_dim_product` 
                 where source_project = 'P360') as product_elix
            ON (CASE WHEN length(ecomm.sap_product_code) > 13 THEN reverse(substr(reverse(ecomm.sap_product_code),1,8))
                     WHEN ecomm.sap_product_code is null or ecomm.sap_product_code='NA' or ecomm.sap_product_code='-1' 
                       or ecomm.sap_product_code = '' THEN '-1'
                     WHEN length(ecomm.sap_product_code) < 8 THEN lpad(ecomm.sap_product_code,8,'0')
                     ELSE trim(ecomm.sap_product_code)
                END) = product_elix.product_code
          LEFT
          JOIN (select * from `spmena-onedata-dna-apac-pd.cds_spmena_sellout_eu.vw_dim_product` 
                 where source_project = 'P360') as product_ecc
            ON (case when ecomm.market_code in ('IN')
                     then 'P65' || '_' || ecomm.sap_product_code
                     when ecomm.market_code in ('AU', 'NZ', 'MY', 'SG', 'TH', 'ID', 'VN', 'PH', 'LB', 'PK', 'EG', 'MA')
                     then 'P54' || '_' || CASE 
                           WHEN length(ecomm.sap_product_code) > 13 THEN reverse(substr(reverse(ecomm.sap_product_code),1,8))
                           WHEN ecomm.sap_product_code is null or ecomm.sap_product_code = 'NA' 
                             or ecomm.sap_product_code = '-1' or ecomm.sap_product_code = '' THEN '-1'
                           WHEN length(ecomm.sap_product_code) < 8 THEN lpad(ecomm.sap_product_code,8,'0')
                           ELSE trim(ecomm.sap_product_code)
                         END
                     when ecomm.market_code in ('SA', 'AE', 'QA', 'KW', 'BH', 'OM', 'JO', 'IQ')
                     then 'P05' || '_' || CASE 
                           WHEN length(ecomm.sap_product_code) > 13 THEN reverse(substr(reverse(ecomm.sap_product_code),1,8))
                           WHEN ecomm.sap_product_code is null or ecomm.sap_product_code = 'NA' 
                             or ecomm.sap_product_code = '-1' or ecomm.sap_product_code = '' THEN '-1'
                           WHEN length(ecomm.sap_product_code) < 8 THEN lpad(ecomm.sap_product_code,8,'0')
                           ELSE trim(ecomm.sap_product_code)
                         END
                  end)=product_ecc.product_code
          LEFT
          JOIN `spmena-onedata-dna-apac-pd.sdds_spmena_product_eu.dim_group_signature` AS dim_grp
            ON LTRIM(ecomm.sap_signature_code, "0") = LTRIM(dim_grp.signature_code, "0")
         WHERE upper(ecomm.period) = 'D'
           AND CASE WHEN ecomm.market_code = 'AU' and shipped_units IS NOT NULL 
                    THEN ecomm.shipped_units = 0 OR ecomm.shipped_revenue = 0
                    ELSE ordered_units IS NULL AND (ecomm.ordered_units = 0 OR ecomm.ordered_revenue = 0)
                END
           AND extract(year from date_id) >= 2021
    ),
    
    sds as (
        SELECT market_code
             , division_code
             , date_id as sellout_date
             , source_project
             , UPPER(CASE WHEN ecomm.market_code is not null and ecomm.client is not null 
                          THEN  CONCAT(ecomm.market_code,'_',upper(ecomm.client),'_',coalesce(ecomm.channel_type,'NA'))
                          ELSE '-1'
                      END) AS point_of_sale_code
             , CASE WHEN ecomm.market_code is not null and ecomm.division_code <>'-1' and ecomm.client is not null
                    THEN CONCAT (ecomm.market_code,'_',ecomm.division_code,'_',upper(ecomm.client),'_',ecomm.period,'_','SO')
                    ELSE '-1'
                END AS data_source
          FROM src as ecomm
    )
    
    SELECT distinct fct.market_code
         , case when fct.division_code='ACD' then 'LDB' else fct.division_code end as division_code
         , fct.source_project
         , fct.point_of_sale_code
         , sellout_date
         , client_name
         , channel_level1
         , channel_level2
      FROM sds as fct
      JOIN `spmena-onedata-dna-apac-pd.cds_spmena_sellout_eu.vw_dim_point_of_sales` as dim_pos
        ON fct.point_of_sale_code = dim_pos.point_of_sale_code
     WHERE concat(fct.division_code, "_", fct.point_of_sale_code, "_", client_name, "_", fct.source_project) 
             in (select * from active_stores)
),

ECOMM_SKU_TRAFFIC_SALES_ZERO as (     
    with src as (
        SELECT market_code
             , case when division='ACD' then 'LDB' else division end as division_code
             , date_id
             , case when (ifnull(market_code, '') = '' or ifnull(client, '') = '') then '-1'
                    else concat(market_code,'_',client,'_',coalesce(platform_store_id, 'NA'))
                end as point_of_sale_code
             , case when  (market_code is null or division = '-1' or client is null or period is null ) then '-1'
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
         , EXTRACT(year FROM  fct.sellout_date) AS year
         , EXTRACT(month FROM fct.sellout_date) AS month
         , client_name
         , source_project
         , channel_level1
         , channel_level2
         , array_agg(distinct extract(day from sellout_date) order by extract(day from sellout_date)) as available_array
      FROM ((SELECT * except(data_source_frequency) FROM vw_fact_sellout WHERE data_source_frequency = 'Daily') 
             UNION ALL
             SELECT * FROM ecom_amz_qty_zero
             UNION ALL
             SELECT * except(sales, units, orders) FROM ECOMM_SKU_TRAFFIC_SALES_ZERO)as fct
     GROUP BY ALL
),

available_weekly_data as (
    SELECT DISTINCT fct.market_code
         , fct.division_code
         , fct.point_of_sale_code
         , extract(year from week_start_date) as year 
         , extract(month from week_start_date) as month
         , fct.client_name
         , fct.source_project
         , fct.channel_level1
         , fct.channel_level2
         , array_agg(week_num order by week_num) as available_array
      FROM vw_fact_sellout as fct
      JOIN vw_dim_calendar       as dim_cal
        ON fct.sellout_date = dim_cal.cal_date
     WHERE sellout_date between date_trunc(date('2021-01-01'), week(Monday))+ 7
                            and date_trunc(date_trunc(current_date(), month), week(sunday))
       AND data_source_frequency = 'Weekly'
     GROUP BY ALL
),

available_monthly_data AS (
    SELECT DISTINCT fct.market_code
         , fct.division_code
         , fct.point_of_sale_code
         , EXTRACT(year FROM  fct.sellout_date) AS year
         , EXTRACT(month FROM fct.sellout_date) AS month
         , client_name
         , fct.source_project
         , channel_level1
         , channel_level2
      FROM vw_fact_sellout as fct
     WHERE data_source_frequency = 'Monthly'
),

--calculating missing data
daily_missing_data as (
    SELECT mrk.market_code
         , mrk.division_code
         , mrk.point_of_sale_code
         , mrk.client_name
         , mrk.source_project
         , mrk.year
         , mrk.month
         , mrk.channel_level1
         , mrk.channel_level2
         , mrk.data_source_frequency
         , array_length(array( select 1 from unnest(mrk.expected_array) a 
                               where a not in (select b from unnest(fct.available_array) b) )) missing_file_count
      FROM expected_daily_data as mrk
      LEFT
      JOIN available_daily_data as fct
     USING ( division_code, point_of_sale_code, year, month, source_project, client_name ) 
     GROUP BY ALL
),

weekly_missing_data as (
    SELECT mrk.market_code
         , mrk.division_code
         , mrk.point_of_sale_code
         , mrk.client_name
         , mrk.source_project
         , mrk.year
         , mrk.month
         , mrk.channel_level1
         , mrk.channel_level2
         , mrk.data_source_frequency
         , array_length(array( select 1 from unnest(mrk.expected_array) a 
                               where a not in (select b from unnest(fct.available_array) b) )) missing_file_count
     FROM expected_weekly_data  as mrk
     LEFT
     JOIN (select * from available_weekly_data) as fct
    USING (division_code, point_of_sale_code, year, month,  source_project, client_name)
    GROUP BY ALL
),

monthly_missing_data as (
    SELECT mrk.market_code
         , mrk.division_code
         , mrk.point_of_sale_code
         , mrk.client_name
         , mrk.source_project
         , mrk.year
         , mrk.month
         , mrk.channel_level1
         , mrk.channel_level2
         , mrk.data_source_frequency
         , (COUNT(mrk.source_project) - COUNT(fct.source_project)) AS missing_file_count
      FROM expected_monthly_data as mrk
      LEFT
      JOIN available_monthly_data as fct
     USING (division_code, point_of_sale_code, year, month, source_project, client_name)
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
      FROM (
      SELECT * FROM daily_missing_data
      UNION ALL
      SELECT * FROM weekly_missing_data
      UNION ALL
      SELECT * FROM monthly_missing_data
      )
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
         , 2 as missing_file
      FROM vw_dim_calendar as dim_cal
     CROSS
      JOIN market_dtl   as mrk     
     WHERE dim_cal.cal_date < DATE_TRUNC(effective_store_start_date, month)
),

final as (
SELECT * FROM total_missing_data
UNION ALL
SELECT * FROM before_store_start_date
)

SELECT *  FROM final
-- ORDER BY missing_file desc, source_project, client_name, point_of_sale_code, division_code, year, month
;

END
