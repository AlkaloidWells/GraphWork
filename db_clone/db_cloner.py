from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from connector.connect import DBConnectionLocal, DBConnectionOnline

def clone_table_structure(table_name):
    try:
        # Connect to online and local databases
        online_conn = DBConnectionOnline().create_db_connection()
        local_conn = DBConnectionLocal().create_db_connection()

        
        print(f"Cloning structure of table {table_name}...")

        # Fetch table structure from the online database
        with online_conn.connect() as online_connection:
            table_creation_query = online_connection.execute(text(f"SHOW CREATE TABLE {table_name}")).fetchone()[1]

        # Use the connection to execute SQL commands in the local database
        with local_conn.connect() as local_connection:
            inspector = inspect(local_connection)

            # Drop the table in the local database if it already exists
            if inspector.has_table(table_name):
                print(f"Table {table_name} already exists. Dropping and updating...")
                local_connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))

            # Create table structure in the local database
            local_connection.execute(text(table_creation_query))
            print(f"Table {table_name} structure copied successfully.")

    except SQLAlchemyError as e:
        print(f"Error during table structure cloning: {str(e)}")
    finally:
        print("Table structure cloning completed.")



def clone_table_data(table_name):
    try:
        # Connect to online and local databases (new connections)
        online_conn = DBConnectionOnline().create_db_connection()
        local_conn = DBConnectionLocal().create_db_connection()

        print(f"Cloning data for table {table_name}...")

        # Copy data from the online database
        with online_conn.connect() as online_connection:
            inspector_online = inspect(online_conn)
            columns = [col['name'] for col in inspector_online.get_columns(table_name)]
            # Fetch data from the online table
            data = online_connection.execute(text(f"SELECT * FROM {table_name}")).fetchall()

            if data:
                print(f"Fetched {len(data)} rows from {table_name}.")

                # Insert data into the local database
                column_list = ', '.join(columns)
                placeholders = ', '.join([f":{col}" for col in columns])  # Named placeholders
                insert_query = f"INSERT INTO {table_name} ({column_list}) VALUES ({placeholders})"

                with local_conn.connect() as local_connection:
                    with local_connection.begin() as transaction:
                        local_connection.execute(text("SET FOREIGN_KEY_CHECKS=0"))  # Disable foreign key checks temporarily
                        try:
                            for row in data:
                                row_dict = {col: row[idx] for idx, col in enumerate(columns)}

                                # Try to insert the row and catch any integrity errors
                                try:
                                    #print(f"Inserting row: {row_dict}")
                                    local_connection.execute(text(insert_query), row_dict)
                                except SQLAlchemyError as insert_error:
                                    print(f"Error during data insert for row {row_dict}: {str(insert_error)}")
                                    # Skip this row due to error
                                    continue
                            print(f"Data copied for table {table_name}.")
                        except SQLAlchemyError as overall_error:
                            print(f"Error during data cloning: {str(overall_error)}")
                            transaction.rollback()  # Rollback on error
                        finally:
                            local_connection.execute(text("SET FOREIGN_KEY_CHECKS=1"))  # Re-enable foreign key checks

            else:
                print(f"No data found for table {table_name}.")

    except SQLAlchemyError as e:
        print(f"Error during data cloning: {str(e)}")
    finally:
        print("Data cloning completed.")




def verify_data(table_name):
    try:
        # Connect to the local database
        local_conn = DBConnectionLocal().create_db_connection()

        print(f"Verifying data for table {table_name}...")

        # Query the first 5 rows from the local table
        with local_conn.connect() as local_connection:
            result = local_connection.execute(text(f"SELECT * FROM {table_name} LIMIT 5")).fetchall()

            if result:
                print(f"First 5 rows from {table_name} in the local database:")
                for row in result:
                    print(row)
            else:
                print(f"No data found in {table_name}.")

    except SQLAlchemyError as e:
        print(f"Error during data verification: {str(e)}")
    finally:
        print("Data verification completed.")
        

if __name__ == "__main__":
    
    
    tables_to_clone = [
            'locations', 'countries','users', 'vendors', 'categories', 'products', 'vendor_view_logs', 'vendor_Product_views',
            'user_subscriptions', 'user_roles', 'sellam_products', 'sellam_orders',
            'search_logs',  'product_whole_sales', 'product_view_logs',
            'category_view_logs',  'buy_logs'
        ]
    
    for table in tables_to_clone:
    
        # Step 1: Clone the structure of the users table
        clone_table_structure(table)

        # Step 2: Close the connection (handled automatically in the functions)
        print("Connection closed after structure cloning. \n")

        # Step 3: Clone the data after reopening connections
        clone_table_data(table)
        print('\n\n')

    # Step 4: Verify that data was inserted into the local database
    #verify_data('vendors')
