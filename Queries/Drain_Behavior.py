
import pandas as pd
from Queries.get_gds_connection import get_gds_connection



def drain_behavior():
    with get_gds_connection() as gds:
        gds.set_database("neo4j")

        gds.run_cypher("""
            MATCH (a:Account)-[t1:TRANSACTION]->(b:Account)-[t2:TRANSACTION]->(c:Account)
            WHERE 
                t2.step - t1.step <= 100
                AND t2.step >= t1.step  
                AND t1.amount > 50000
                AND (t1.newbalanceOrig / CASE WHEN t1.oldbalanceOrg = 0 THEN 1 
                    ELSE t1.oldbalanceOrg END) < 0.4  
                AND t2.amount > 50000
                AND (t2.newbalanceOrig / CASE WHEN t2.oldbalanceOrg = 0 THEN 1 
                    ELSE t2.oldbalanceOrg END) < 0.4  
            set a += {is_drain_behavior : true},
                b += {is_drain_behavior : true},
                c += {is_drain_behavior : true}
            """)
        

