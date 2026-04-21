fan_in_query = """
MATCH (a:Account)-[:TRANSACTION]->(b:Account)
WITH b, count(DISTINCT a) AS fan_in_count
RETURN b.id AS account_id, fan_in_count
ORDER BY fan_in_count DESC
LIMIT 25;
"""
