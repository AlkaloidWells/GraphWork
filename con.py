from connector.connect import DBConnectionOnline
from connector.connect import DBConnectionLocal
from sqlalchemy import inspect

def test_db_connection():
    db = DBConnectionLocal()
    connection = db.create_db_connection()
    
    # Use SQLAlchemy's inspect to get table names
    inspector = inspect(connection)
    
    # Get list of all tables and print them
    tables = inspector.get_table_names()
    
    if tables:
        print("Tables in the database:")
        for table in tables:
            print(table)
    else:
        print("No tables found in the database.")

# Call the function to test the connection
test_db_connection()