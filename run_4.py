from py2neo import Graph
import pandas as pd

# Recommend products viewed by similar users and return as DataFrame
def recommend_products_viewed_by_similar_users(graph, target_user_id, limit=5):
    query = """
    MATCH (target:User {user_id: $target_user_id})-[:VIEWED]->(p:Product)<-[:VIEWED]-(similar:User)-[:VIEWED]->(recommended:Product)
    WHERE NOT (target)-[:VIEWED]->(recommended)
    RETURN recommended.product_id AS product_id, count(similar) AS frequency
    ORDER BY frequency DESC
    LIMIT $limit
    """
    result = graph.run(query, target_user_id=target_user_id, limit=limit)
    data = pd.DataFrame([dict(record) for record in result])
    return data

# Recommend users who viewed/bought/searched the same products as the target user and return as DataFrame
def recommend_similar_users_by_product_interactions(graph, target_user_id, limit=5):
    query = """
    MATCH (target:User {user_id: $target_user_id})-[:VIEWED|:BOUGHT|:SEARCHED]->(p:Product)<-[:VIEWED|:BOUGHT|:SEARCHED]-(similar:User)
    WHERE target <> similar
    RETURN similar.user_id AS similar_user, count(p) AS common_interactions
    ORDER BY common_interactions DESC
    LIMIT $limit
    """
    result = graph.run(query, target_user_id=target_user_id, limit=limit)
    data = pd.DataFrame([dict(record) for record in result])
    return data

# Recommend users for a given product category and return as DataFrame
def recommend_users_for_product_category(graph, category_id, limit=5):
    query = """
    MATCH (u:User)-[:VIEWED|:BOUGHT|:SEARCHED]->(p:Product)-[:BELONGS_TO]->(c:Category {category_id: $category_id})
    RETURN u.user_id AS user_id, count(p) AS interaction_count
    ORDER BY interaction_count DESC
    LIMIT $limit
    """
    result = graph.run(query, category_id=category_id, limit=limit)
    data = pd.DataFrame([dict(record) for record in result])
    return data

# Recommend recently viewed but not purchased products and return as DataFrame
def recommend_recently_viewed_not_purchased(graph, target_user_id, limit=5):
    query = """
    MATCH (user:User {user_id: $target_user_id})-[:VIEWED]->(p:Product)
    WHERE NOT (user)-[:BOUGHT]->(p)
    RETURN p.product_id AS product_id
    ORDER BY p.timestamp DESC
    LIMIT $limit
    """
    result = graph.run(query, target_user_id=target_user_id, limit=limit)
    data = pd.DataFrame([dict(record) for record in result])
    return data




def recommend_products(user_id, limit=10):
    query = """
    MATCH (target_user:User {user_id: $user_id})
    
    // Products bought by users who bought the same products as the target user
    OPTIONAL MATCH (target_user)-[:BOUGHT]->(p:Product)<-[:BOUGHT]-(other_user:User)-[:BOUGHT]->(rec_product:Product)
    WHERE NOT (target_user)-[:BOUGHT]->(rec_product)
    
    WITH target_user, rec_product, COUNT(*) AS bought_score
    
    // Products viewed by users who viewed the same products as the target user
    OPTIONAL MATCH (target_user)-[:VIEWED]->(p:Product)<-[:VIEWED]-(other_user:User)-[:VIEWED]->(rec_product:Product)
    WHERE NOT (target_user)-[:VIEWED]->(rec_product)
    
    WITH target_user, rec_product, bought_score, COUNT(*) AS view_score
    
    // Products searched by users who searched similar queries
    OPTIONAL MATCH (target_user)-[:SEARCHED]->(p:Product)<-[:SEARCHED]-(other_user:User)-[:SEARCHED]->(rec_product:Product)
    WHERE NOT (target_user)-[:SEARCHED]->(rec_product)
    
    WITH rec_product, bought_score, view_score, COUNT(*) AS search_score
    
    // Sum up the scores based on interactions (give more weight to buying)
    RETURN rec_product.product_id AS product_id,
           bought_score * 3 + view_score * 2 + search_score AS total_score
    ORDER BY total_score DESC
    LIMIT $limit
    """

    recommendations = graph.run(query, user_id=user_id, limit=limit).data()
    
    return recommendations




if __name__ == "__main__":
 
    graph = Graph("bolt://localhost:7687", auth=("neo4j", "$Verine123"))
    
    # Example usage of recommendation functions
    target_user_id = 292  # Replace with actual user_id
    category_id = 8       # Replace with actual category_id

    print("Products viewed by similar users:\n", recommend_products_viewed_by_similar_users(graph, target_user_id),"\n")
    print("Users with similar product interactions:\n", recommend_similar_users_by_product_interactions(graph, target_user_id),"\n")
    print("Users for category recommendation:\n", recommend_users_for_product_category(graph, category_id))
    print("Recently viewed but not purchased products:\n", recommend_recently_viewed_not_purchased(graph, target_user_id),"\n")
