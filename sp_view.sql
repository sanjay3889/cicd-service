BEGIN
create temp table temp_results(
  market_code string,
  division_code string,
  point_of_sale_code STRING,
    client_name STRING,
    source_project STRING,
    YEAR INT64,
    MONTH INT64,
    channel_level1 STRING,
    channel_level2 STRING,
    data_source_frequency STRING,
    missing_file_count INT64,
    missing_file INT64
);

  INSERT INTO temp_results
select * from `oa-apmena-itdelivery-sp-np.Test_dataset.missing_sp`();

  CREATE OR REPLACE VIEW `oa-apmena-itdelivery-sp-np.Test_dataset.vw_dynamic_result` AS
  SELECT *
  FROM temp_result;
END;
