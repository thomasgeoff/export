-- https://jira.bancvue.com/browse/REGDEV-1032
-- https://confluence.bancvue.com/display/DEV/NII+Subscriber+Summary+Data+Contract

SELECT
      '{current_month_last_date}'   AS `period_date`
    , financial_institution_id      AS `financial_institution_id`
    , provider_reference            AS `provider_reference`
    , provider_name                 AS `provider_name`
    , subscription_level            AS `subscription_level`
    , fi_rev_share_recurring_rate   AS `referral_rate`
    , COUNT(1)                      AS `quantity`
    , '{creation_date_time}'        AS `file_creation_date_time`
FROM (
    SELECT
          provs.provider_reference
        , provs.provider_name
        , provs.fi_rev_share_recurring_rate
        , CONCAT(provs.provider_name, ' ', subs.product_type) AS subscription_level
        , IFNULL(subs.financial_institution_id, 0) AS financial_institution_id
    FROM subscriptions subs
        INNER JOIN providers provs
            ON provs.provider_id = subs.provider_id
    WHERE subs.subscriber_id > ''
        -- the effective effect dates are in the current month's full range first of month to last (active subscriptions)
        -- OR the end of term date is in the future
        AND ((YEAR(subs.effective_date) = YEAR(CURRENT_DATE()) AND MONTH(subs.effective_date) = MONTH(CURRENT_DATE()))
            OR CURRENT_DATE() < DATE_ADD(subs.effective_date, INTERVAL IFNULL(CAST(subs.term AS SIGNED), 0) MONTH))

    UNION

    SELECT
          provs.provider_reference
        , provs.provider_name
        , provs.fi_rev_share_recurring_rate
        , CONCAT(provs.provider_name, ' ', subs.product_type) AS subscription_level
        , IFNULL(subs.financial_institution_id, 0) AS financial_institution_id
    FROM payments pays
        INNER JOIN subscriptions subs
            ON subs.subscription_id = pays.subscription_id
        INNER JOIN providers provs
            ON provs.provider_id = subs.provider_id
    WHERE pays.payment_id > ''
        -- the payment dates are in the current month's full range first of month to last (active payments)
        AND YEAR(pays.payment_date) = YEAR(CURRENT_DATE()) AND MONTH(pays.payment_date) = MONTH(CURRENT_DATE())
) AS base_data
GROUP BY provider_reference, provider_name, subscription_level, financial_institution_id, fi_rev_share_recurring_rate;
