import pandas as pd
from Queries.get_gds_connection import get_gds_connection



def fan_out():
    with get_gds_connection() as gds:
        gds.set_database("neo4j")

        gds.run_cypher("""
            MATCH (a:Account)
            CALL (a){
            OPTIONAL MATCH (a)-[t:TRANSACTION]->(b:Account)
            WITH a, count(DISTINCT b) AS fan_out_count, sum(t.amount) AS total_out_amount
            SET a += {
                fan_out_count: fan_out_count,
                total_out_amount: coalesce(total_out_amount, 0)
            }
            WITH a, fan_out_count, total_out_amount
            WHERE fan_out_count >= 20 AND total_out_amount > 50000
            SET a.is_fan_out = true
            } IN TRANSACTIONS OF 1000 ROWS
            """)