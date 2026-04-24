"""
Download the PaySim dataset, strip the fraud-flag columns, write a clean
copy to the working directory, and delete the cached download.
"""
from neo4j import GraphDatabase
from pathlib import Path
from dotenv import load_dotenv

import os
from Queries.Large_transfers import find_large_transfers_with_cashout_after
from Queries.Drain_Behavior import drain_behavior
from Queries.get_gds_connection import get_gds_connection
from Queries.FanIn import fan_in
from Queries.FanOut import fan_out
from Queries.DenseCommunity import dense_community
from Queries.GuiltByAssociation import guilt_by_association
from Queries.NodeSimilarity import  node_similarity
from Queries.FlagFraud import flag_all_fraud
from Queries.RiskScore import risk_score
import time

load_dotenv()



def main():
    
    driver = GraphDatabase.driver(
        str(os.getenv("db_uri")),
        auth=(str(os.getenv("db_user")), str(os.getenv("db_password"))))


    
    with driver.session() as session:
        session.run ("CREATE INDEX FOR (a:Account) ON (a.id);")
        print("loading dataset")
        session.run("\
            LOAD CSV WITH HEADERS FROM 'File:///paysim_clean.csv' AS row\
            CALL {\
            WITH row\
            MERGE (a:Account {id: row.nameOrig, is_cycle: false, is_drain_behavior: false, is_fan_in: false, is_fan_out: false, is_large_transfer: false, is_dense_community:false, is_guilty:false, is_similar:false, is_fraud:false})\
            MERGE (b:Account {id: row.nameDest, is_cycle: false, is_drain_behavior: false, is_fan_in: false, is_fan_out: false, is_large_transfer: false, is_dense_community:false, is_guilty:false, is_similar:false, is_fraud:false})\
            CREATE (a)-[:TRANSACTION {\
                type: row.type,\
                amount: toFloat(row.amount),\
                step: toInteger(row.step),\
                oldbalanceOrg: toFloat(row.oldbalanceOrg),\
                newbalanceOrig: toFloat(row.newbalanceOrig),\
                oldbalanceDest: toFloat(row.oldbalanceDest),\
                newbalanceDest: toFloat(row.newbalanceDest),\
                isFraud: toInteger(row.isFraud),\
                isFlaggedFraud: toInteger(row.isFlaggedFraud)\
            }]->(b)\
            } IN TRANSACTIONS OF 5000 ROWS\
            "
        )
        
    driver.close()
    

    time.sleep(2)
    print("Finding large transfers with cashout")
    find_large_transfers_with_cashout_after()
    time.sleep(2)
    print("Finding drain behavior")
    drain_behavior()
    time.sleep(2)
    print("Finding fan in")
    fan_in()
    time.sleep(2)
    print("Finding fan out")
    fan_out()
    time.sleep(2)

    print("Dense Community")
    dense_community()
    time.sleep(2)

    print("Flagging all fraud with is fraud")
    flag_all_fraud()
    time.sleep(2)

    print("Guilt by association")
    guilt_by_association()
    time.sleep(2)

    print("Node similarity")
    node_similarity()
    time.sleep(2)
    
    print("Risk Score")
    risk_score()


if __name__ == "__main__":
    main()