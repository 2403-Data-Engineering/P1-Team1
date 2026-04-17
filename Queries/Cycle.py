"""
Script for finding cycles and updating fraudulent transactions
"""

from  graphdatascience import GraphDataScience
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import os

'''
match path = (a:Account)-[:TRANSACTION *3..10]->(b:Account)
RETURN path
most likely b is fraud
'''
"""
match path = ()-[*0..10]-(b:Account)
where b.id = "C934752135"
RETURN path
"""

load_dotenv()

with GraphDataScience(str(os.getenv("db_uri")), auth=(str(os.getenv("db_user")), str(os.getenv("db_password")))) as gds:
    gds.set_database("neo4j")



    row = gds.run_cypher("match (a:Account)-[*1..100]-(a) return count(a)")
    gds.run_cypher("")
    for r in row.to_numpy():
        print(r)
    row = gds
    print(gds.version())

