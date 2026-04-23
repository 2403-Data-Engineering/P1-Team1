"""
Script for finding draining
"""


import pandas as pd
from Queries.get_gds_connection import get_gds_connection



def find_large_transfers_with_cashout_after():
    with get_gds_connection() as gds:
        gds.set_database("neo4j")

        gds.run_cypher("""
            MATCH (a:Account)-[t1:TRANSACTION {type: 'TRANSFER'}]->(b:Account)-[t2:TRANSACTION {type: 'CASH_OUT'}]->(c:Account)
            where ((t2.step - t1.step <= 2) or (t1.step - t2.step <= 2)) AND abs(t1.amount - t2.amount) / t1.amount < 0.1
            SET a += {is_large_transfer: true},
            b += {is_large_transfer: true},
            c += {is_large_transfer: true}
            """)
        
