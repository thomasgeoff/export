-- https://jira.bancvue.com/browse/REGDEV-1032
-- https://confluence.bancvue.com/pages/viewpage.action?pageId=67629719

SELECT
    '{current_month_last_date}' AS `period_date`
    , financial_institution_id  AS `financial_institution_id`
    , subscriber_reference      AS `subscriber_reference`
    , effective_date            AS `first_acquired_date`
    , current_status            AS `current_status`
    , subscription_level        AS `subscription_level`
    , '{creation_date_time}'    AS `file_creation_date_time`
FROM (
    SELECT
          IFNULL(subs.financial_institution_id, 0) AS financial_institution_id
        , subs.subscriber_id AS subscriber_reference  -- this needs to be the original field from the vendor for subscriber identification
        , subs.effective_date
        , CASE
            WHEN ((YEAR(subs.effective_date) = YEAR(CURRENT_DATE()) AND MONTH(subs.effective_date) = MONTH(CURRENT_DATE()))
                OR CURRENT_DATE() < DATE_ADD(subs.effective_date, INTERVAL IFNULL(CAST(subs.term AS SIGNED), 0) MONTH))
            THEN 'ACTIVE'
            ELSE 'INACTIVE' END AS current_status
        , CONCAT(provider_name, ' ', product_type) AS subscription_level
    FROM subscriptions subs
        INNER JOIN providers provs
            ON provs.provider_id = subs.provider_id
            AND provs.provider_name = 'Experian'  -- this can be restricted to just Experian data
    WHERE subs.subscriber_id > ''
) AS base_data;
