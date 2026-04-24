





from Queries.get_gds_connection import get_gds_connection



def risk_score():
    with get_gds_connection() as gds:
        gds.set_database("neo4j")

        gds.run_cypher("""
            MATCH (a)
            with a,
                (CASE WHEN a.is_guilty           = true THEN 10 ELSE 0 END +
                CASE WHEN a.is_cycle            = true THEN 10 ELSE 0 END +
                CASE WHEN a.is_fan_in           = true THEN 10 ELSE 0 END +
                CASE WHEN a.is_fan_out          = true THEN 10 ELSE 0 END +
                CASE WHEN a.is_large_transfer   = true THEN 15 ELSE 0 END +
                CASE WHEN a.is_drain_behavior   = true THEN 15 ELSE 0 END +
                CASE WHEN a.is_dense_community  = true THEN 20 ELSE 0 END +
                CASE WHEN a.is_similar          = true THEN 10 ELSE 0 END 
                )as risk_score
            call(a, risk_score){
            set a += {risk_score: risk_score}
            } in TRANSACTIONS of 1000 rows
            """)
        






