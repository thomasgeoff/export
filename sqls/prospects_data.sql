SELECT
      prospect_kasasa_key
    , financial_institution_id
    , branch_id
    , kasasa_master_person_id
    , email
    , email_append_flag
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
    , advantage_target_narrow_band_income
    , advantage_target_income_indicator
    , house_hold_type_code
    , geo_code_census_tract_block_number_area
    , geo_code_block_group
    , geo_code_census_state_code
    , geo_code_census_county_code
    , geo_code_neilsen_county_size_code
    , geo_code_dma_code
    , advantage_dwelling_type
    , advantage_dwelling_type_indicator
    , individual_age_ccyymm
    , distance_to_branch
    , time_zone
    , score
    , wireless_flag
    , decile
    , phone_number
    , expiration_date
    , 'FICO' AS vendor_name
    , fico_individual_id AS vendor_sourced_person_individual_id
    , DATE_FORMAT(data_load_date, "%Y-%m-%d") AS Source_System_Data_Load_Date
    , UTC_TIMESTAMP() AS file_creation_date_time
FROM fi_{fi_id}.fico_sourced_prospects;