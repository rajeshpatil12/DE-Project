# import pyodbc

# # Define the connection string
# connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=your_server;DATABASE=your_database;UID=your_username;PWD=your_password'

# # Connect to the SQL Server database
# connection = pyodbc.connect(connection_string)

# try:
#     with connection.cursor() as cursor:
#         # Execute a SQL query
#         cursor.execute("SELECT * FROM your_table_name")
        
#         # Fetch the result
#         for row in cursor:
#             print(row)
# finally:
#     connection.close()

from pyodbc import create_engine

# SQL Server connection details
server = 'your_server.database.windows.net'
database = 'your_database'
username = 'your_username'
password = 'your_password'
driver = 'ODBC Driver 17 for SQL Server'

# Create the connection string
connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}"

# Create an engine for connecting to the SQL Server
engine = create_engine(connection_string)
