

from Queries.get_gds_connection import get_gds_connection



def guilt_by_association():
    with get_gds_connection() as gds:
        gds.set_database("neo4j")

        gds.run_cypher("""
            match (a:Account {is_fraud:false})-[t:TRANSACTION]->(b:Account {is_fraud:true})
            with a,count(DISTINCT b) AS bad_neighbors 
            WHERE bad_neighbors >= 2
            CAll (a){
            set a += {is_guilty:true, is_fraud: true}
            }in TRANSACTIONS of 1000 rows
            """)
        

