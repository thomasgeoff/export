SELECT
      customer_kasasakey
    , financial_institution_id
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
    , email
    , phone_number
    , 'livetech' AS vendor
    , cm_birthday
    , cm_anniversary
    , cm_welcome
    , cm_adoption
    , cm_retention
    , cm_cross_sell_email
    , cm_cross_sell_direct
    , cm_cross_sell_win_back
    , cm_cross_sell_k_protect
    , fico_individual_id
    , load_date_time
    , UTC_TIMESTAMP()
FROM map_campaign.livetech_customers
WHERE financial_institution_id = {fi_id}
    AND load_date_time = (
            SELECT MAX(load_date_time)
            FROM map_campaign.livetech_customers
            WHERE financial_institution_id = {fi_id}
                AND DATE(load_date_time) = DATE(UTC_TIMESTAMP())
        )
;