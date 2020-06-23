SELECT
      prospect_kasasa_key
    , customer_kasasa_key
    , financial_institution_id
    , 'FICO' AS vendor_name
    , DATE_FORMAT(data_load_date, "%Y-%m-%d")
    , UTC_TIMESTAMP() AS file_creation_date_time
FROM fi_{fi_id}.prospect_customer_mapping