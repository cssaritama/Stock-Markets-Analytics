import dlt
import requests
import json
from qdrant_client import QdrantClient
from dlt.destinations import qdrant

@dlt.resource
def zoomcamp_data():
    """Resource function to yield documents from the Zoomcamp FAQ"""
    docs_url = 'https://github.com/alexeygrigorev/llm-rag-workshop/raw/main/notebooks/documents.json'
    docs_response = requests.get(docs_url)
    documents_raw = docs_response.json()

    for course in documents_raw:
        course_name = course['course']
        for doc in course['documents']:
            doc['course'] = course_name
            yield doc

def run_pipeline():
    """Run the dlt pipeline to load data into Qdrant"""
    qdrant_destination = qdrant(
        qd_path="db.qdrant",
    )

    pipeline = dlt.pipeline(
        pipeline_name="zoomcamp_pipeline",
        destination=qdrant_destination,
        dataset_name="zoomcamp_tagged_data"
    )

    load_info = pipeline.run(zoomcamp_data())
    print("Pipeline run successfully!")
    print(load_info)
    return pipeline

def get_embedding_model():
    """Check which embedding model was used"""
    with open("db.qdrant/meta.json", "r") as f:
        meta_data = json.load(f)
    return meta_data["sources"]["zoomcamp_pipeline"]["resources"]["zoomcamp_data"]["config"]["embedder"]["model_name"]

if __name__ == "__main__":
    # Ejecutar el pipeline
    pipeline = run_pipeline()
    
    # Obtener información del modelo de embeddings
    print("\nEmbedding model used:", get_embedding_model())
    
    # Mostrar estadísticas de carga
    print("\nLast trace:", pipeline.last_trace)