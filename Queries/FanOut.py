fan_out_query = """
MATCH (a:Account)-[:TRANSACTION]->(b:Account)
WITH a, count(DISTINCT b) AS fan_out_count
RETURN a.id AS account_id, fan_out_count
ORDER BY fan_out_count DESC
LIMIT 25;
"""
