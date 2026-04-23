

from Queries.get_gds_connection import get_gds_connection



def drain_behavior():
    with get_gds_connection() as gds:
        gds.set_database("neo4j")

        gds.run_cypher("""
            match (a:Account)-[t:TRANSACTION]->(b:Account)
            WHERE b.is_cycle or b.is_dense_community or b.is_drain_behavior or b.is_fan_in or b.is_fan_out or b.is_large_transfer
            CAll (a){
            set a += {is_guilty:true}
            }in TRANSACTIONS of 1000 rows
            """)
        

