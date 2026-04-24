

from Queries.get_gds_connection import get_gds_connection



def flag_all_fraud():
    with get_gds_connection() as gds:
        gds.set_database("neo4j")

        gds.run_cypher("""
            match (a:Account)
            WHERE a.is_cycle or a.is_dense_community or a.is_drain_behavior or a.is_fan_in or a.is_fan_out or a.is_large_transfer
            CAll (a){
            set a += {is_fraud: true}
            }in TRANSACTIONS of 1000 rows
            """)
        

