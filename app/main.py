import pandas as pd
import numpy as np
import faiss
from fastapi import FastAPI, Query
from pydantic import BaseModel
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import os

from code.etl.gold import GOLD_BUCKET, GOLD_FILE_NAME


USE_FAISS = True  # Set to False to use pure cosine similarity (in-memory)

import sys
import os
from more_itertools import bucket
from numpy import save
import pandas as pd
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))
from utils.minio_helper import save_dataframe_as_parquet_to_minio, read_parquet_from_minio

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../'))

load_dotenv(dotenv_path=os.path.join(PROJECT_ROOT, ".env.dev"))


GOLD_BUCKET: str = os.getenv("GOLD_BUCKET", "")
GOLD_FILE_NAME: str = os.getenv("GOLD_FILE_NAME", "")

file_name = GOLD_FILE_NAME if GOLD_FILE_NAME.endswith('.parquet') else f"{GOLD_FILE_NAME}.parquet"

df = read_parquet_from_minio(GOLD_BUCKET, file_name)


# Validate structure
assert 'embedding' in df.columns and 'paragraph' in df.columns, "Missing required columns"

# Prepare data
texts = df['paragraph'].tolist()
metadatas = df[['book_title', 'author', 'source_url']].to_dict(orient='records')
embeddings = np.vstack(df['embedding'].values).astype("float32")

# Load embedder (same used during ingestion)
model = SentenceTransformer("all-MiniLM-L6-v2")

# Build FAISS index
if USE_FAISS:
    print("⚡ Building FAISS index...")
    dim = embeddings.shape[1]
    index = faiss.IndexFlatL2(dim)
    index.add(embeddings)

# FastAPI app
app = FastAPI(title="Simple RAG App", version="1.0")

@app.get("/ask")
def ask(question: str = Query(...), top_k: int = 3):
    query_embedding = model.encode([question]).astype("float32")

    if USE_FAISS:
        D, I = index.search(query_embedding, top_k)
        indices = I[0]
    else:
        sims = cosine_similarity(query_embedding, embeddings)[0]
        indices = np.argsort(sims)[-top_k:][::-1]

    results = []
    for idx in indices:
        results.append({
            "paragraph": texts[idx],
            "metadata": metadatas[idx]
        })

    return {
        "question": question,
        "top_k": top_k,
        "results": results
    }

