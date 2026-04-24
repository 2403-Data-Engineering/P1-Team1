import os
import pandas
import dotenv
from dotenv import load_dotenv
import neo4j
from graphdatascience import GraphDataScience
from Queries.get_gds_connection import get_gds_connection
import time
"""
Level: ring
Pass: first
What it is: A cluster of accounts moving lots of money among its own members.

How to find it: Use gds.louvain.write to assign every account a community_id property. Then aggregate with Cypher: for each community, compute total transaction volume within the community (both endpoints share the same community_id) and total volume leaving the community. A high internal-to-external ratio combined with small community size (3–15 accounts) is the ring signature. Flag communities that cross a threshold on both metrics.
"""
def node_similarity():
    load_dotenv()

    database = str(os.getenv("db_name"))
    community_id = "community_id"
    graph_name = "Node_Similarity"
    # create a graph so we can add the community ids
    graph_query = """
MATCH (source:Account)-[t:TRANSACTION]-(target:Account)
where source.is_fraud
RETURN gds.graph.project(
  "fraudSimilarityGraph",
  source,
  target
)
"""

    neighbor_query='''
    CALL gds.nodeSimilarity.write('fraudSimilarityGraph', {
  similarityCutoff:      0.1,
  topK:                  10,
  degreeCutoff:          2,
  concurrency: 4,
  writeConcurrency: 4,
  writeRelationshipType: 'SIMILAR',
  writeProperty:         'score'
})
'''

    flag_query='''
    match (a)-[t:SIMILAR]-(b)
    WHERE t.score > 0.7
    CALL(a){
    set a += {is_similar : true, is_fraud: true}
    } in TRANSACTIONS of 1000 rows
    
    '''
  


    with get_gds_connection() as gds:
            gds.set_database(database)


            print("\tSetting Graph")
            time.sleep(2)
            gds.run_cypher(query=graph_query)
        
            print("\tSetting neighbor relationship")
            time.sleep(2)
            gds.run_cypher(query=neighbor_query)

            print("\tSetting neighbor flag")
            time.sleep(2)
            gds.run_cypher(query=flag_query)








'''
CALL gds.nodeSimilarity.write('fraudSimilarityGraph', {
  similarityCutoff:      0.1,
  topK:                  10,
  degreeCutoff:          2,
  concurrency: 4,
  writeConcurrency: 4,
  writeRelationshipType: 'SIMILAR',
  writeProperty:         'score'
})
YIELD
  nodesCompared,
  relationshipsWritten,
  similarityDistribution


  
MATCH (source:Account)-[t:TRANSACTION]-(target:Account)
where source.is_fraud
RETURN gds.graph.project(
  "fraudSimilarityGraph",
  source,
  target
)

'''

