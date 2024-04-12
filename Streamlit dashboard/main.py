import streamlit as st
from sentence_transformers import SentenceTransformer
import os
import pandas as pd
import weaviate

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
        additional_headers={
            "X-OpenAI-Api-Key": os.environ["OPENAI_API_KEY"]
        }
    )
    # Get the collection to search
    qdrant.get_collection(collection_name=collectionname)

except Exception as e:
    print(f"Exception: {e}")

# Create a text input field for the search term
search_term = st.text_input("Enter your search term")
search_result = []

if search_term != "":

    try:
        # Vectorize the search term
        query_vector = encoder.encode([search_term])[0]

        total_points = qdrant.get_collection(collection_name=collectionname).points_count
        if total_points == 0:
            st.write("Collection is empty")
        else:
            # Query the database
            # Example of a semantic search using nearText search for quiz objects similar to "biology"
            nearText = {"concepts": ["biology"]}
            response = (
                client.query
                .get("Question", ["question", "answer", "category"])
                .with_near_text(nearText)
                .with_limit(2)
                .do()
            )
            print(json.dumps(response, indent=4))

            # Initialize a list to hold each row of data
            resultdata = []

            # Iterate through the search results
            for result in search_result:
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
            st.markdown('**Qdrant Search Results**')

            if len(search_result) < 1:
                print("No matches")
            else:
                st.write(df)

    except Exception as e:
        print(f"Exception: {e}")