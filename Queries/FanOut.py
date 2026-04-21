MATCH (a:Account)-[t:TRANSACTION]->(b:Account)
WITH a, count(DISTINCT b) AS fan_out_count, sum(t.amount) AS total_out_amount
SET a.fan_out_count = fan_out_count
SET a.total_out_amount = total_out_amount
SET a.fan_out_flag = CASE
    WHEN fan_out_count >= 3 AND total_out_amount > 50000 THEN 1
    ELSE 0
END
WITH a
WHERE a.fan_out_flag = 1
RETURN a.id AS fraudulent_account_id, a.fan_out_count, a.total_out_amount, a.fan_out_flag
ORDER BY a.total_out_amount DESC
