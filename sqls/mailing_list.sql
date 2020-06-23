SELECT 
      customer_kasasakey
    , REPLACE(list_name, '[Entity]', '') AS list_name  -- strip out the [Entity] tag for EDW
    , financial_institution_id
    , load_date_time
FROM map_campaign.livetech_mailing_list
WHERE financial_institution_id = {fi_id}
    AND load_date_time = (
            SELECT MAX(load_date_time)
            FROM map_campaign.livetech_mailing_list
            WHERE financial_institution_id = {fi_id}
                AND DATE(load_date_time) = DATE(UTC_TIMESTAMP())
        )
;
