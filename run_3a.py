from py2neo import Graph, Node, Relationship
from db_clone.connector.connect import *
import pandas as pd
import plotly.graph_objs as go
import networkx as nx
import plotly.io as pio
import os

# Establish connection to Neo4j
graph = Graph("bolt://localhost:7687", auth=("neo4j", "$Verine123"))

# Fetch data from MySQL (assuming it's the same as before)
def fetch_data_from_mysql():
    db = DBConnectionLocal()
    engine = db.create_db_connection()
    
    query = """
        WITH vendor_products AS (
            SELECT p.id AS product_id, p.category_id
            FROM products p
            JOIN vendors v ON p.user_id = v.user_id
            WHERE v.user_id = 268  -- Replace with the actual vendor_id
        ),
        users_interacted AS (
            SELECT DISTINCT user_id 
            FROM product_view_logs
            JOIN vendor_products vp ON vp.product_id = product_view_logs.product_id
            
            UNION

            SELECT DISTINCT user_id 
            FROM buy_logs
            JOIN vendor_products vp ON vp.product_id = buy_logs.product_id
        )

        -- Retrieve views
        SELECT 
            pvl.user_id, 
            pvl.product_id, 
            vp.category_id, 
            'view' AS action_type 
        FROM product_view_logs pvl
        JOIN vendor_products vp ON vp.product_id = pvl.product_id

        UNION  -- Remove duplicates

        -- Retrieve buys
        SELECT 
            bl.user_id, 
            bl.product_id, 
            vp.category_id, 
            'buy' AS action_type 
        FROM buy_logs bl
        JOIN vendor_products vp ON vp.product_id = bl.product_id

        UNION  -- Remove duplicates

        -- Retrieve searches only from users who interacted with the vendor's products
        SELECT 
            sl.user_id, 
            NULL AS product_id,  -- No product_id for search logs
            NULL AS category_id,  -- No category_id for search logs
            'search' AS action_type 
        FROM search_logs sl
        WHERE sl.user_id IN (SELECT user_id FROM users_interacted);
    """

    data = pd.read_sql(query, engine)
    return data.to_dict(orient='records')

# Insert data into Neo4j
def insert_data_into_neo4j(results):
    for index, row in enumerate(results):
        print(f"Inserting record {index + 1}/{len(results)}: {row}")
        try:
            user_node = Node("User", user_id=row["user_id"])
            product_node = Node("Product", product_id=row["product_id"])
            category_node = Node("Category", category_id=row["category_id"])

            graph.merge(user_node, "User", "user_id")
            graph.merge(product_node, "Product", "product_id")
            graph.merge(category_node, "Category", "category_id")

            if row['action_type'] == 'view':
                graph.create(Relationship(user_node, "VIEWED", product_node))
            elif row['action_type'] == 'search':
                graph.create(Relationship(user_node, "SEARCHED", product_node))
            elif row['action_type'] == 'buy':
                graph.create(Relationship(user_node, "BOUGHT", product_node))

            graph.create(Relationship(product_node, "BELONGS_TO", category_node))

            print(f"Record {index + 1} inserted successfully.")
        except Exception as e:
            print(f"Error inserting record {index + 1}: {e}")

# Improved Visualization Function
# Improved Visualization Function
def visualize_graph_plotly(graph, max_nodes=100, output_filename="graph_visualization.png"):
    G = nx.Graph()

    query = f"MATCH (u:User)-[r]->(p:Product) RETURN u.user_id AS user, r, p.product_id AS product LIMIT {max_nodes}"
    for record in graph.run(query):
        user = record["user"]
        product = record["product"]
        G.add_node(user, label="User")
        G.add_node(product, label="Product")
        G.add_edge(user, product, label=str(record["r"].type))

    pos = nx.spring_layout(G, seed=42)  # Improved layout for consistency and readability

    # Extract edge and node data for Plotly
    edge_x = []
    edge_y = []
    edge_labels = []
    for edge in G.edges(data=True):  # Include edge data
        x0, y0 = pos[edge[0]]  # Start position of the edge
        x1, y1 = pos[edge[1]]  # End position of the edge
        edge_x.append(x0)
        edge_x.append(x1)
        edge_x.append(None)
        edge_y.append(y0)
        edge_y.append(y1)
        edge_y.append(None)
        edge_labels.append(edge[2]['label'])  # Get the edge label

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=1.5, color='gray'),
        hoverinfo='none',
        mode='lines'
    )

    node_x = []
    node_y = []
    for node in G.nodes():
        x, y = pos[node]  # Position of the node
        node_x.append(x)
        node_y.append(y)

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        text=[f"User: {n}" if G.nodes[n]['label'] == 'User' else f"Product: {n}" for n in G.nodes()],
        textposition="top center",
        marker=dict(
            showscale=True,
            colorscale='YlGnBu',
            size=10,
            color=[],
            line_width=2
        ),
        hoverinfo='text'
    )

    # Adding node colors based on degree (number of edges)
    node_adjacencies = []
    for node in G.nodes():
        node_adjacencies.append(len(list(G.neighbors(node))))
    node_trace.marker.color = node_adjacencies

    fig = go.Figure(data=[edge_trace, node_trace],
                    layout=go.Layout(
                        title='User-Product Interaction Graph',
                        titlefont_size=16,
                        showlegend=False,
                        hovermode='closest',
                        margin=dict(b=0, l=0, r=0, t=40),
                        annotations=[dict(
                            text="User-Product Graph Visualization",
                            showarrow=False,
                            xref="paper", yref="paper",
                            x=0.005, y=-0.002
                        )],
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))

    # Add edge labels
    for i, (x0, y0, x1, y1, label) in enumerate(zip(edge_x[::3], edge_y[::3], edge_x[1::3], edge_y[1::3], edge_labels)):
        mid_x = (x0 + x1) / 2
        mid_y = (y0 + y1) / 2
        fig.add_annotation(
            x=mid_x,
            y=mid_y,
            text=label,
            showarrow=False,
            font=dict(size=10)
        )

    # Save the plot as an image
    pio.write_image(fig, output_filename)
    print(f"Graph saved as {output_filename}")

    fig.show()

# Usage:


# Main function
if __name__ == "__main__":
    print("Fetching data from MySQL...")
    results = fetch_data_from_mysql()  # Fetch data from MySQL
    print(f"Fetched {len(results)} records.")

    print("Inserting data into Neo4j...")
    insert_data_into_neo4j(results)

    print("Visualizing graph...")
    visualize_graph_plotly(graph, max_nodes=200, output_filename="user_product_graph_limited.png")
    
  