from py2neo import Graph, Node, Relationship
import pandas as pd
from db_clone.connector.connect import *
import plotly.graph_objs as go
import networkx as nx

# Establish connection to Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "$Verine123"))


def fetch_data_from_mysql():
    # Assuming you have a MySQL connection set up using SQLAlchemy
    # and you can replace `DBConnectionLocal` with your actual connection class
    db = DBConnectionLocal()
    engine = db.create_db_connection()

    # Query to fetch data from product_view_logs, search_logs, and buy_logs along with category details
    query = """
            SELECT 
                product_view_logs.user_id, 
                product_view_logs.product_id, 
                products.category_id, 
                'view' AS action_type 
            FROM product_view_logs
            JOIN products ON products.id = product_view_logs.product_id

            UNION

            SELECT 
                search_logs.user_id, 
                NULL AS product_id,  -- No product_id for search logs
                NULL AS category_id,  -- No category_id for search logs
                'search' AS action_type 
            FROM search_logs

            UNION

            SELECT 
                buy_logs.user_id, 
                buy_logs.product_id, 
                products.category_id, 
                'buy' AS action_type 
            FROM buy_logs
            JOIN products ON products.id = buy_logs.product_id;

    """

    data = pd.read_sql(query, engine)
    return data.to_dict(orient='records')


def insert_data_into_neo4j(results):
    for index, row in enumerate(results):
        print(f"Inserting record {index + 1}/{len(results)}: {row}")
        # Create user, product, category nodes
        try:
            user_node = Node("User", user_id=row["user_id"])
            product_node = Node("Product", product_id=row["product_id"])
            category_node = Node("Category", category_id=row["category_id"])

            # Add nodes to graph
            graph.merge(user_node, "User", "user_id")
            graph.merge(product_node, "Product", "product_id")
            graph.merge(category_node, "Category", "category_id")

            # Create relationships based on action_type (view, search, buy)
            if row['action_type'] == 'view':
                graph.create(Relationship(user_node, "VIEWED", product_node))
            elif row['action_type'] == 'search':
                graph.create(Relationship(user_node, "SEARCHED", product_node))
            elif row['action_type'] == 'buy':
                graph.create(Relationship(user_node, "BOUGHT", product_node))

            # Create the BELONGS_TO relationship for products and categories
            graph.create(Relationship(product_node, "BELONGS_TO", category_node))

            print(f"Record {index + 1} inserted successfully.")
        except Exception as e:
            print(f"Error inserting record {index + 1}: {e}")

        

def visualize_graph_plotly(graph):
    G = nx.Graph()

    # Add nodes and edges from Neo4j data
    for record in graph.run("MATCH (u:User)-[r]->(p:Product) RETURN u.user_id AS user, r, p.product_id AS product"):
        user = record["user"]
        product = record["product"]
        G.add_node(user, label="User")
        G.add_node(product, label="Product")
        G.add_edge(user, product, label=str(record["r"].type))

    # Calculate positions for the nodes using a layout algorithm
    pos = nx.spring_layout(G)  # You can also use other layouts like nx.kamada_kawai_layout(G), etc.

    # Extract edge and node data for Plotly
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]  # Start position of the edge
        x1, y1 = pos[edge[1]]  # End position of the edge
        edge_x.append(x0)
        edge_x.append(x1)
        edge_x.append(None)  # None to create a break between edges
        edge_y.append(y0)
        edge_y.append(y1)
        edge_y.append(None)

    # Create the edge trace
    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='gray'),
        hoverinfo='none',
        mode='lines')

    # Create the node trace
    node_x = []
    node_y = []
    for node in G.nodes():
        x, y = pos[node]  # Position of the node
        node_x.append(x)
        node_y.append(y)

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        text=list(G.nodes()),
        textposition="top center",
        marker=dict(
            showscale=True,
            colorscale='YlGnBu',
            size=10,
            color=[],
            line_width=2))

    fig = go.Figure(data=[edge_trace, node_trace],
                     layout=go.Layout(
                        title='User-Product Interaction Graph',
                        titlefont_size=16,
                        showlegend=False,
                        hovermode='closest',
                        margin=dict(b=0, l=0, r=0, t=40),
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))

    fig.show()





# Call this function after inserting data into Neo4j



if __name__ == "__main__":
    print("hello")
    results = fetch_data_from_mysql()  # Fetch the data from MySQL
    print("hello 2")
    insert_data_into_neo4j(results)
    print(f"Number of records fetched: {len(results)}")# Insert the fetched data into Neo4j
    visualize_graph_plotly(graph)
