SELECT
      CASE
        WHEN prospect_kasasa_key IS NULL THEN CONCAT(financial_institution_id, lac_consumer_link)
        ELSE prospect_kasasa_key
      END as prospect_kasasa_key
    , financial_institution_id
    , branch_id
    , CASE
        WHEN kasasa_master_person_id IS NULL THEN REPLACE(TO_BASE64(SHA2(lac_consumer_link, 256)), '\n', '_')
        ELSE kasasa_master_person_id
      END as kasasa_master_person_id
    , email
    , NULL as email_append_flag
    , full_name
    , first_name
    , middle_name
    , last_name
    , NULL as suffix
    , address_1
    , address_2
    , city
    , `state`
    , zipcode
    , zipcode_extended
    , delivery_point_bar_code
    , carrier_route_code
    , line_of_travel
    , NULL as national_change_of_address_move_flag
    , NULL as national_change_of_address_move_date
    , NULL as national_change_of_address_drop_flag
    , NULL as advantage_target_narrow_band_income
    , NULL as advantage_target_income_indicator
    , NULL as house_hold_type_code
    , NULL as geo_code_census_tract_block_number_area
    , NULL as geo_code_block_group
    , NULL as geo_code_census_state_code
    , NULL as geo_code_census_county_code
    , NULL as geo_code_neilsen_county_size_code
    , NULL as geo_code_dma_code
    , NULL as advantage_dwelling_type
    , NULL as advantage_dwelling_type_indicator
    , age
    , distance_to_branch
    , NULL as time_zone
    , NULL as score
    , NULL as wireless_flag
    , NULL as decile
    , NULL as phone_number
    , expiration_date
    , 'Acxiom' AS vendor_name
    , lac_consumer_link AS vendor_sourced_person_individual_id
    , DATE_FORMAT(data_load_date, "%Y-%m-%d") AS Source_System_Data_Load_Date
    , UTC_TIMESTAMP() AS file_creation_date_time
FROM fi_{fi_id}.acxiom_sourced_prospects;