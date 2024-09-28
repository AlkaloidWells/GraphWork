import os
import pandas as pd
from sqlalchemy import create_engine
from py2neo import Graph, Node, Relationship
import networkx as nx
import matplotlib.pyplot as plt
from db_clone.connector.connect import *
import matplotlib
import tkinter as tk
matplotlib.use('TkAgg')


# Step 1: Fetch data from MySQL using DBConnectionLocal
def fetch_data_from_mysql():
    db = DBConnectionLocal()  # Create an instance of the DBConnectionLocal class
    engine = db.create_db_connection()  # Establish connection

    query = """
    SELECT user_id, product_id FROM product_view_logs
    WHERE user_id IS NOT NULL AND user_id != 0
    LIMIT 100;
    """
    
    # Use pandas to execute the query and fetch the data as a DataFrame
    data = pd.read_sql(query, engine)
    return data.to_dict(orient='records')  # Convert the DataFrame to a list of dicts

mysql_data = fetch_data_from_mysql()
print(mysql_data[:5])  # Display some sample data from MySQL

# Step 2: Insert data into Neo4j
def insert_data_into_neo4j(data):
    graph = Graph("bolt://localhost:7687", auth=("neo4j", "$Verine123"))

    for record in data:
        user_node = Node("User", user_id=record['user_id'])
        product_node = Node("Product", product_id=record['product_id'])
        
        # Create VIEWED relationship between user and product
        relationship = Relationship(user_node, "VIEWED", product_node)
        
        graph.merge(user_node, "User", "user_id")
        graph.merge(product_node, "Product", "product_id")
        graph.create(relationship)

insert_data_into_neo4j(mysql_data)

# Step 3: Query data from Neo4j
def get_graph_data():
    graph = Graph("bolt://localhost:7687", auth=("neo4j", "$Verine123"))
    query = """
    MATCH (u:User)-[r:VIEWED]->(p:Product)
    RETURN u.user_id AS user, p.product_id AS product
    LIMIT 100
    """
    results = graph.run(query).data()
    return results

graph_data = get_graph_data()

# Step 4: Build NetworkX Graph
def build_networkx_graph(data):
    G = nx.Graph()
    
    for record in data:
        user = record['user']
        product = record['product']
        G.add_node(user, label="User")
        G.add_node(product, label="Product")
        G.add_edge(user, product, label="VIEWED")

    return G

# Step 5: Visualize the graph using NetworkX
import plotly.graph_objects as go

def visualize_interactive_graph(G):
    pos = nx.spring_layout(G)
    
    # Create edges
    edge_x = []
    edge_y = []
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.append(x0)
        edge_x.append(x1)
        edge_x.append(None)
        edge_y.append(y0)
        edge_y.append(y1)
        edge_y.append(None)

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines')

    # Create nodes
    node_x = []
    node_y = []
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        hoverinfo='text',
        marker=dict(
            showscale=True,
            colorscale='YlGnBu',
            size=20,
            colorbar=dict(
                thickness=15,
                title='Node Connections',
                xanchor='left',
                titleside='right'
            ),
        ),
        text=[f"User: {n}" if G.nodes[n]['label'] == 'User' else f"Product: {n}" for n in G.nodes()]
    )

    fig = go.Figure(data=[edge_trace, node_trace],
                    layout=go.Layout(
                        title='Interactive User-Product Graph',
                        titlefont_size=16,
                        showlegend=False,
                        hovermode='closest',
                        margin=dict(b=0, l=0, r=0, t=40),
                        annotations=[dict(
                            text="User-Product Graph",
                            showarrow=False,
                            xref="paper", yref="paper",
                            x=0.005, y=-0.002
                        )],
                        xaxis=dict(showgrid=False, zeroline=False),
                        yaxis=dict(showgrid=False, zeroline=False))
                    )
    
    fig.show()




# Fetch and visualize the graph
nx_graph = build_networkx_graph(graph_data)
visualize_interactive_graph(nx_graph)
