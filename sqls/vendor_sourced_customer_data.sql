SELECT 
      financial_institution_id
    , customer_kasasa_key
    , kasasa_master_person_id
    , full_name
    , first_name
    , middle_name
    , last_name
    , suffix
    , address_1
    , address_2
    , city
    , state
    , zipcode
    , zipcode_extended
    , delivery_point_bar_code
    , carrier_route_code
    , line_of_travel
    , national_change_of_address_move_flag
    , CASE 
        WHEN national_change_of_address_move_date = '0000-00-00' THEN NULL
        ELSE DATE_FORMAT(national_change_of_address_move_date, '%Y%m') -- YYYYMM
      END AS national_change_of_address_move_date
    , national_change_of_address_drop_flag
    , email
    , email_append_flag
    , 'FICO' AS vendor_name
    , fico_individual_id AS vendor_sourced_person_individual_id
    , DATE_FORMAT(data_load_date, "%Y-%m-%d") AS source_system_data_load_date
    , UTC_TIMESTAMP() AS file_creation_date_time
FROM fi_{fi_id}.fico_sourced_customers;