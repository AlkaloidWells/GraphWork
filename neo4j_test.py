from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "$Verine123"))

def test_connection():
    try:
        with driver.session() as session:
            result = session.run("MATCH (n) RETURN n LIMIT 1")
            for record in result:
                print(record)
        print("Connection successful!")
    except ServiceUnavailable as e:
        print(f"Service Unavailable: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    test_connection()
