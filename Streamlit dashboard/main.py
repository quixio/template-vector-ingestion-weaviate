import streamlit as st
from sentence_transformers import SentenceTransformer
import os
import pandas as pd
import weaviate
import json

# Initialize the sentence transformer model
encoder = SentenceTransformer('all-MiniLM-L6-v2')  # Model to create embeddings
collectionname = os.environ['collectionname']

st.title('Simple Vector Database Search')

st.markdown('Search a Qdrant Cloud database for matches to a query (it can take a few seconds to return a result).')


# Perform the process here
try:
    print(f"Using collection name {collectionname}")

    # Initialize the QdrantClient
    # Initialize the Weaviate client. Replace the placeholder values with your actual Weaviate instance details.
    client = weaviate.Client(
        url=os.environ["weaviate_url"],
        auth_client_secret=weaviate.AuthApiKey(api_key=os.environ["WEAVIATE_API KEY"]),
    )
    # Get the collection to search
    # qdrant.get_collection(collection_name=collectionname)

except Exception as e:
    print(f"Exception: {e}")

# Create a text input field for the search term
search_term = st.text_input("Enter your search term")
search_result = []

if search_term != "":

    try:
        # Vectorize the search term
        query_vector = encoder.encode([search_term])[0]

        #total_points = qdrant.get_collection(collection_name=collectionname).points_count
        total_points = 12 # hardcoded for testing
        if total_points == 0:
            st.write("Collection is empty")
        else:
            # Query the database
            # Example of a semantic search using nearText search for quiz objects similar to "biology"
            result = (
                    client.query
                    .get("BookCatalog", ["title", "description"])
                    .with_near_vector({
                        "vector": query_vector,
                        "certainty": 0.7
                    })
                    .with_limit(2)
                    .do()
                )
                
            print(json.dumps(result, indent=4))

            # Initialize a list to hold each row of data
            resultdata = []

            # Iterate through the search results
            for res in result:
                # Extracting data from each result
                row = {
                    'name': result.payload['name'],
                    'description': result.payload['description'],
                    'score': result.score,
                    'author': result.payload['author'],
                    'year': str(result.payload['year']),
                    'id': result.id,
                }
                resultdata.append(row)
            df = pd.DataFrame(resultdata)

            print(f"Total points stored {total_points}")
            # Display the results in a Streamlit app
            st.markdown('**Weaviate Search Results**')

            if len(search_result) < 1:
                print("No matches")
            else:
                st.write(df)

    except Exception as e:
        print(f"Exception: {e}")