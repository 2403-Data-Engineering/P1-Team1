import pandas as pd
from Queries.get_gds_connection import get_gds_connection



def fan_in():
    with get_gds_connection() as gds:
        gds.set_database("neo4j")

        gds.run_cypher(
        """
            match (b:Account)
            CALL (b){
            MATCH (a:Account)-[t:TRANSACTION]->(b)
            WITH b, count(DISTINCT a) AS fan_in_count, sum(t.amount) AS total_in_amount
            SET b += {fan_in_count : fan_in_count, total_in_amount: total_in_amount}
            WITH b, fan_in_count, total_in_amount
            WHERE total_in_amount > 3000000 and fan_in_count >= 20
            SET b.is_fan_in = true
            } in TRANSACTIONS of 1000 rows
        """
        )