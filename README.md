# P1-Team1

Members: Luke Po, John Magri, Silas Bucur, Guilherme Vilatoro Taglianeti

Neo4J setup:
Run the download_data.py script
Create a new instance of neo4j
Add the paysim_clean.csv file to the import folder of the newly created instance.
Run the database_creation.py script to initialize the DB.


Node : Accounts - nameOrig / nameDest
    Holds all the other data

Edges : Transactions
    Payment, Transfer, Cash_out, Debit
    Holds all the other data
