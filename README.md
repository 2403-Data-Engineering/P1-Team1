# P1-Team1

Members: Luke Po, John Magri, Guilherme Vilatoro Taglianeti

- Neo4J setup:
  - Run the download_data.py script
  - Create a new instance of neo4j
  - Add the paysim_clean.csv file to the import folder of the newly created instance.
  - Run the database_creation.py script to initialize the DB.
  - After the data is added to neo4j turn off the instance and intall the Graph Data Science plugin

- Python setup:
  - Run python -m venv .venv  
  - Run pip install -r requirements.txt


Node : Accounts - nameOrig / nameDest
    Holds all the other data

Edges : Transactions
    Payment, Transfer, Cash_out, Debit
    Holds all the other data
