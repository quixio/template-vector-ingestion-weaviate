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

encoder = SentenceTransformer('all-MiniLM-L6-v2') # Model to create embeddings
collection = os.environ['collectionname']

# Create collection to store items
qdrant.recreate_collection(
    collection_name=collection,
    vectors_config=models.VectorParams(
        size=encoder.get_sentence_embedding_dimension(), # Vector size is defined by used model
        distance=models.Distance.COSINE
    )
)

# Define the ingestion function
def ingest_vectors(row):

  single_record = models.PointStruct(
    id=row['doc_uuid'],
    vector=row['embeddings'],
    payload=row
    )

  qdrant.upload_points(
      collection_name=collection,
      points=[single_record]
    )

  print(f'Ingested vector entry id: "{row["doc_uuid"]}"...')

app = Application(
    "vectorizer",
    auto_offset_reset="earliest",
    auto_create_topics=True,  # Quix app has an option to auto create topics
)

# Define an input topic with JSON deserializer
input_topic = app.topic(os.environ['input'], value_deserializer="json") # Merlin.. i updated this for you

# Initialize a streaming dataframe based on the stream of messages from the input topic:
sdf = app.dataframe(topic=input_topic)

# INGESTION HAPPENS HERE
### Trigger the embedding function for any new messages(rows) detected in the filtered SDF
sdf = sdf.update(lambda row: ingest_vectors(row))
app.run(sdf)