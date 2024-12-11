INSERT INTO `{prj_data_quality}.{dst_data_quality}.{tbl_dq_run_result}`
(dq_id, run_timestamp, attribute_arr, dq_score, invalid_rows, total_rows, invalid_value, total_value, param_date)
with params as (
    SELECT cast(@DQ_ID         as STRING)    as dq_id
         , cast(@RUN_TIMESTAMP as TIMESTAMP) as run_timestamp
         , cast(@PARAM_DATE    as STRING)    as param_date
),
{% set parent_data = ({
    'DISTRIBUTOR.distributor_code'      : ['itg-btdppublished-gbl-ww-'~env~'.btdp_ds_c1_613_customerprofile_eu_'~env~'.general_data_v1', 'business_partner_number_external_system'],
    'DISTRIBUTOR.distributor_name'      : ['itg-btdppublished-gbl-ww-'~env~'.btdp_ds_c1_613_customerprofile_eu_'~env~'.general_data_v1', 'organization_legal_name_1'],
    'CURRENCY.conversion_rate'          : ['spmena-onedata-dna-apac-'~env~'.sdds_spmena_finance_eu.vw_fact_exchange_rate_v1', 'exchange_rate'],
    'GEOGRAPHY.orga_region_description' : ['spmena-onedata-dna-apac-'~env~'.sdds_spmena_common_eu.vw_dim_market', 'zone_code'],
    'PRODUCT.orga_division'             : ['spmena-onedata-dna-apac-'~env~'.sdds_spmena_product_eu.dim_product_v1', 'division_code'],
    'PRODUCT.signature_code'            : ['spmena-onedata-dna-apac-'~env~'.sdds_spmena_product_eu.dim_product_v1', 'signature_code'],
    'PRODUCT.signature_desc'            : ['spmena-onedata-dna-apac-'~env~'.sdds_spmena_product_eu.dim_product_v1', 'signature_name'],
    'PRODUCT.axis_code'                 : ['spmena-onedata-dna-apac-'~env~'.sdds_spmena_product_eu.dim_product_v1', 'axe_code'],
    'PRODUCT.axis_desc'                 : ['spmena-onedata-dna-apac-'~env~'.sdds_spmena_product_eu.dim_product_v1', 'axe_name'],
    'PRODUCT.brand_code'                : ['spmena-onedata-dna-apac-'~env~'.sdds_spmena_product_eu.dim_product_v1', 'brand_code'],
    'PRODUCT.brand_desc'                : ['spmena-onedata-dna-apac-'~env~'.sdds_spmena_product_eu.dim_product_v1', 'brand_name'],
    'PRODUCT.subaxis_code'              : ['spmena-onedata-dna-apac-'~env~'.sdds_spmena_product_eu.dim_product_v1', 'sub_axe_code'],
    'PRODUCT.subaxis_desc'              : ['spmena-onedata-dna-apac-'~env~'.sdds_spmena_product_eu.dim_product_v1', 'sub_axe_name'],
    'PRODUCT.subbrand_code'             : ['spmena-onedata-dna-apac-'~env~'.sdds_spmena_product_eu.dim_product_v1', 'sub_brand_code'],
    'PRODUCT.subbrand_desc'             : ['spmena-onedata-dna-apac-'~env~'.sdds_spmena_product_eu.dim_product_v1', 'sub_brand_name']
})
%}

src_data as (
    SELECT division_amaas                                          as division_code
         , GEOGRAPHY.orga_country_alpha2                           as market_code
         , CAST(EXTRACT(YEAR FROM tech_period_end_date) AS STRING) AS sit_year
         , DISTRIBUTOR.distributor_name                            as client_name
         , METADATA_TECHNICAL.data_file_provider                   as data_source
         , 'Distributor'                                           as source_type
         , {{source_column}}                                       as source_column
         , abs(FACT_VALUE.total_stock_units)                       as total_units
      from `spmena-onedata-dna-apac-{{env}}.sdds_stock_in_trade_eu.stock_in_trade_sapmena_distributor_v1`
      where EXTRACT(YEAR FROM tech_period_end_date)=2024 
),
ref_data as (
    {% set parent_table = parent_data[source_column][0] %}
    {% set parent_column = parent_data[source_column][1] %}
    SELECT distinct {{parent_column}} as parent_column
      from `{{parent_table}}`
),
cols_calculated as (
    select fct.division_code
         , market_code  
         , sit_year
         , data_source
         , source_type
         , client_name
         , sum(case when dim.parent_column IS NULL or fct.source_column IS NULL or fct.source_column = '' or fct.source_column = '-1'  or fct.source_column = 'unspecified'
                    then total_units else 0 end) as invalid_value
         , sum(total_units) as total_value
         , count(case when dim.parent_column IS NULL or fct.source_column IS NULL or fct.source_column = '' or fct.source_column = '-1'  or fct.source_column = 'unspecified'
                      then 1 else null end) as invalid_rows
         , count(1) as total_rows
      from src_data as fct
      left 
      join ref_data as dim
        on fct.source_column = dim.parent_column
     group by 1, 2, 3, 4, 5, 6
),
final_cte as (
    SELECT dq_id
         , run_timestamp
         , [
              struct('division_code'  as key, division_code  as value),
              struct('market_code'    as key, market_code    as value),
              struct('sit_year'       as key, sit_year       as value),
              struct('data_source'    as key, data_source    as value),
              struct('source_type'    as key, source_type    as value),
              struct('client_name'    as key, client_name    as value)
           ] as key_value
         , cast(case when total_rows is not null and total_rows <> 0 then (1 - (invalid_rows / total_rows)) * 100       end as numeric) as dq_score
         , invalid_rows
         , total_rows
         , invalid_value
         , total_value
         , param_date
      FROM params
     CROSS
      JOIN cols_calculated
)
select * from final_cte
;
