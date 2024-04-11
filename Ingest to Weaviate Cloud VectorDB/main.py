from quixstreams import Application
from sentence_transformers import SentenceTransformer
import weaviate
import os

#  Initialize the Weaviate client. Replace the placeholder values with your actual Weaviate instance details.
client = weaviate.Client(
    url="https://quix-template-viv8pz43.weaviate.network",
    auth_client_secret=weaviate.AuthApiKey(api_key=os.environ["WEAVIATE_API KEY"]),
    additional_headers={
        "X-OpenAI-Api-Key": os.environ["OPENAI_API_KEY"]
    }
)

collection = os.environ['collectionname']

# Create a class AKA "collection"
if client.schema.exists(collection):
    client.schema.delete_class(collection)

# Configure the class AKA "collection"
class_obj = {
    "class": collection,
    "vectorizer": "none", # Not using Weaviates in-built vectorization to demonstrate decoupling of vector creation from ingestion
    "moduleConfig": {
        "generative-openai": {}  # Use `generative-openai` module for generative queries
    }
}

# Define the ingestion function
def ingest_vectors(row):

  recordid = client.data_object.create(
    class_name="QuestionV",
    data_object={
        "answer": row['answer'],
        "category": row['category'],
        "question": row['question'],
    },
    vector = [0.12345] * 1536
    )

  print(f'Ingested vector record id: "{recordid}"...')

app = Application(
    "vectorizer",
    auto_offset_reset="earliest",
    auto_create_topics=True,  # Quix app has an option to auto create topics
)

# Define an input topic with JSON deserializer
input_topic = app.topic(os.environ['input'], value_deserializer="json")

# Initialize a streaming dataframe based on the stream of messages from the input topic:
sdf = app.dataframe(topic=input_topic)

# INGESTION HAPPENS HERE
### Trigger the embedding function for any new messages(rows) detected in the filtered SDF
sdf = sdf.update(lambda row: ingest_vectors(row))
app.run(sdf)