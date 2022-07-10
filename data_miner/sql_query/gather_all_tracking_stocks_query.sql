
/* 所有的来自投资标的池（含指数成分股和目标股）的股票' */
INSERT IGNORE INTO  target_pool.all_tracking_stocks_rf
    (stock_code, stock_name, exchange_location, exchange_location_mic)
SELECT DISTINCT stock_code, stock_name, stock_exchange_location as exchange_location, stock_market_code as exchange_location_mic
FROM financial_data.mix_top10_with_bottom_no_repeat
UNION
SELECT DISTINCT target_code as stock_code, target_name as stock_name, exchange_location, exchange_location_mic
FROM target_pool.investment_target
WHERE target_type = 'stock'
ON duplicate KEY UPDATE id = id;