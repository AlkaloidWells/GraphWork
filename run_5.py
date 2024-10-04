import pandas as pd
from db_clone.connector.connect import DBConnectionLocal  # Adjust the import to match your project's structure
from py2neo import Graph, Node, Relationship
import pandas as pd

# Establish a Neo4j connection
graph = Graph("bolt://localhost:7687", auth=("neo4j", "$Verine123"))

def fetch_data_from_mysql():
    db = DBConnectionLocal()
    engine = db.create_db_connection()
    print("Database connection established:", engine)

    # Fetch views
    views_query = """
        SELECT 
            vp.vendor_id, 
            vp.vendor_name,
            u.id AS user_id, 
            u.name AS user_name, 
            pvl.product_id, 
            vp.product_name, 
            vp.category_id, 
            vp.category_name, 
            'view' AS action_type 
        FROM product_view_logs pvl
        JOIN (
            SELECT 
                p.id AS product_id, 
                p.category_id, 
                p.name AS product_name, 
                v.user_id AS vendor_id, 
                v.name AS vendor_name, 
                c.name AS category_name
            FROM products p
            JOIN vendors v ON p.user_id = v.user_id
            JOIN categories c ON p.category_id = c.id
        ) AS vp ON vp.product_id = pvl.product_id
        JOIN users u ON pvl.user_id = u.id
        WHERE pvl.user_id IN (
            SELECT DISTINCT user_id
            FROM (
                SELECT user_id FROM product_view_logs
                UNION
                SELECT user_id FROM buy_logs
            ) AS users_with_activity
        )
    """

    print("Executing views query...")
    try:
        views_data = pd.read_sql(views_query, engine)
        print("Views data count:", views_data.shape[0])
    except Exception as e:
        print("Error fetching views data:", e)

    # Fetch buys
    buys_query = """
        SELECT 
            vp.vendor_id, 
            vp.vendor_name,
            u.id AS user_id, 
            u.name AS user_name, 
            bl.product_id, 
            vp.product_name, 
            vp.category_id, 
            vp.category_name, 
            'buy' AS action_type 
        FROM buy_logs bl
        JOIN (
            SELECT 
                p.id AS product_id, 
                p.category_id, 
                p.name AS product_name, 
                v.user_id AS vendor_id, 
                v.name AS vendor_name, 
                c.name AS category_name
            FROM products p
            JOIN vendors v ON p.user_id = v.user_id
            JOIN categories c ON p.category_id = c.id
        ) AS vp ON vp.product_id = bl.product_id
        JOIN users u ON bl.user_id = u.id
        WHERE bl.user_id IN (
            SELECT DISTINCT user_id
            FROM (
                SELECT user_id FROM product_view_logs
                UNION
                SELECT user_id FROM buy_logs
            ) AS users_with_activity
        )
    """

    print("Executing buys query...")
    try:
        buys_data = pd.read_sql(buys_query, engine)
        print("Buys data count:", buys_data.shape[0])
    except Exception as e:
        print("Error fetching buys data:", e)

    # Fetch searches
    searches_query = """
        SELECT 
            vp.vendor_id, 
            vp.vendor_name,
            u.id AS user_id, 
            u.name AS user_name, 
            NULL AS product_id, 
            NULL AS product_name, 
            NULL AS category_id, 
            NULL AS category_name, 
            'search' AS action_type 
        FROM search_logs sl
        JOIN users u ON sl.user_id = u.id
        JOIN (
            SELECT 
                v.user_id AS vendor_id, 
                v.name AS vendor_name
            FROM vendors v
        ) AS vp ON vp.vendor_id = u.id
        WHERE u.id IN (
            SELECT DISTINCT user_id
            FROM (
                SELECT user_id FROM product_view_logs
                UNION
                SELECT user_id FROM buy_logs
            ) AS users_with_activity
        )
    """

    print("Executing searches query...")
    try:
        searches_data = pd.read_sql(searches_query, engine)
        print("Searches data count:", searches_data.shape[0])
    except Exception as e:
        print("Error fetching searches data:", e)

    # Combine all results
    combined_data = pd.concat([views_data, buys_data, searches_data], ignore_index=True)
    print("Combined data shape:", combined_data.shape)

    return combined_data.to_dict(orient='records')

def insert_data_into_neo4j(results):
    for index, row in enumerate(results):
        print(f"Inserting record {index + 1}/{len(results)}: {row}")
        try:
            # Create User node
            user_node = Node("User", user_id=row["user_id"], user_name=row["user_name"])

            # Create Vendor node (representing the shop)
            vendor_node = Node("Vendor", vendor_id=row["vendor_id"], vendor_name=row["vendor_name"])

            # Merge User and Vendor nodes into the graph
            graph.merge(user_node, "User", "user_id")
            graph.merge(vendor_node, "Vendor", "vendor_id")

            # Handle Product nodes only if product_id exists (product interactions)
            if row['product_id'] is not None:
                # Create Product and Category nodes
                product_node = Node("Product", product_id=row["product_id"], product_name=row["product_name"])
                category_node = Node("Category", category_id=row["category_id"], category_name=row["category_name"])

                # Merge Product and Category nodes into the graph
                graph.merge(product_node, "Product", "product_id")
                graph.merge(category_node, "Category", "category_id")

                # Create relationships for actions (view, buy, etc.)
                graph.create(Relationship(user_node, row["action_type"].upper(), product_node))
                
                # Product belongs to a category
                graph.create(Relationship(product_node, "BELONGS_TO", category_node))

                # Vendor owns the product (one-to-many relationship)
                graph.create(Relationship(vendor_node, "OWNS", product_node))

            # For searches (where product_id is NULL), relate User to Vendor directly
            if row['action_type'] == 'search' and row['product_id'] is None:
                graph.create(Relationship(user_node, "SEARCHED", vendor_node))

            # Always relate the User to the Vendor for all types of interactions
            graph.create(Relationship(user_node, "INTERACTED_WITH", vendor_node))

            print(f"Record {index + 1} inserted successfully.")
        except Exception as e:
            print(f"Error inserting record {index + 1}: {e}")

# Main Execution
if __name__ == "__main__":
    # Fetch records from MySQL database
    records = fetch_data_from_mysql()
    
    # Insert records into Neo4j
    insert_data_into_neo4j(records)

    print("Data insertion complete.")
