--DROP TABLE IF EXISTS SLAVIK0041YANDEXBY__DWH.global_metrics;   
CREATE TABLE IF NOT EXISTS SLAVIK0041YANDEXBY__DWH.global_metrics
    (
        date_update date,
        currency_from int,
        amount_total numeric(18,6),
        cnt_transactions int,
        avg_transactions_per_account numeric(18,6),
        cnt_accounts_make_transactions int
    );
