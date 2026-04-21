import os
import pandas
import dotenv
from dotenv import load_dotenv
import neo4j
from graphdatascience import GraphDataScience
"""
Level: ring
Pass: first
What it is: A cluster of accounts moving lots of money among its own members.

How to find it: Use gds.louvain.write to assign every account a community_id property. Then aggregate with Cypher: for each community, compute total transaction volume within the community (both endpoints share the same community_id) and total volume leaving the community. A high internal-to-external ratio combined with small community size (3–15 accounts) is the ring signature. Flag communities that cross a threshold on both metrics.
"""

load_dotenv()

uri = str(os.getenv("NEO4J_URI"))
username = str(os.getenv("NEO4J_USERNAME"))
password = str(os.getenv("NEO4J_PASSWORD"))
database = str(os.getenv("NEO4J_DATABASE"))
community_id = "community_id"
graph_name = "my_graph"
# create a graph so we can add the community ids
graph_query = """
CALL gds.graph.project(
  '"""+ graph_name + """',
  ['Account'], ['TRANSACTION']
)"""
# add the community ids to each account node
add_community_ids_query = """
CALL gds.louvain.write('"""+ graph_name +"""', {
  writeProperty: 'community_id'
})
YIELD communityCount, modularity
"""

set_community_internal_total = """
MATCH(a:Account)
WITH DISTINCT a.community_id as community
CALL {
  WITH community
  MATCH (a:Account {community_id: community})-[t:TRANSACTION]->(b:Account {community_id: community})
  WHERE t.type <> "CASH_OUT"
  WITH community, sum(t.amount) AS total
  MATCH (acc:Account {community_id: community})
  SET acc.community_total_amount = total
} IN TRANSACTIONS OF 10000 ROWS

"""

set_community_leave_total = """
MATCH(a:Account)
WITH DISTINCT a.community_id as community
CALL {
  WITH community
  MATCH (a:Account {community_id: community})-[t:TRANSACTION {type: "CASH_OUT"}]->(b:Account {community_id: community})
  WITH community, sum(t.amount) AS total
  MATCH (acc:Account {community_id: community})
  SET acc+={community_leave_total: total}
} IN TRANSACTIONS OF 10000 ROWS

"""
# this is to gather a list of all the different communities with sizes 3-15
communities_query = """
MATCH (n)
WITH n.community_id AS community_id, collect(n) AS accounts
WITH community_id, accounts, size(accounts) AS community_size WHERE community_size >= 3 AND community_size <= 15
RETURN community_id, community_size, [node in accounts | id(node)] AS account_ids, [node in accounts | node.community_total_amount] as community_internal_total_amounts, [node in accounts | node.community_leave_total] AS community_leave_total_amounts;
"""
high_internal_to_external_ratio = 2

#TODO: need to flag all accounts which are in communities that have a high internal to external ratio and size between 3 and 15
flag_all_accounts_in_sus_communities = """
MATCH (n)
WITH n.community_id AS community_id, collect(n) AS accounts, n.community_total_amount AS community_internal_total, n.community_leave_total AS community_leave_total
WITH community_id, accounts, size(accounts) AS community_size, [node in accounts | id(node)] AS account_ids, community_internal_total, community_leave_total
WHERE community_size >= 3 AND community_size <= 15
WITH
  community_id,
  community_size,
  CASE 
    WHEN community_leave_total > 0 AND (community_internal_total / community_leave_total) >= """+ str(high_internal_to_external_ratio) +""" THEN TRUE
    ELSE FALSE
  END AS is_dense
MATCH (acc:Account {community_id: community_id})
SET acc.dense_community = is_dense
"""

# find the total transaction volume within the community
# need to get sum of all transactions within the community
# with this query, we have applied the community totals to each account (as a new field kind of)
# 


with GraphDataScience(endpoint=uri, auth=(username, password)) as gds:
    gds.set_database(database)
    gds.run_cypher(query=graph_query)
    community_count, modularity = gds.run_cypher(query=add_community_ids_query)
    
    gds.run_cypher(query=set_community_internal_total)
    gds.run_cypher(query=set_community_leave_total)
    gds.run_cypher(query=flag_all_accounts_in_sus_communities)
    # rows = gds.run_cypher(query=communities_query)
    # community_id_list = []
