import logging
import subprocess
import time
import json
import os

from haystack.document_stores import ElasticsearchDocumentStore
from haystack.nodes import EmbeddingRetriever
from haystack.utils import launch_es
from haystack.utils import launch_es
import pandas as pd
from pathlib import Path
from haystack.nodes import PreProcessor
import numpy as np
import dateutil

logger = logging.getLogger(__name__)

doc_index = "document"
language = "dutch"
n_docs = 2000000
doc_dir = Path("data")
split_length = 100


if __name__ == "__main__":
    print("Main")
    launch_es()
    document_store = ElasticsearchDocumentStore(analyzer=language, index=doc_index, timeout=300, similarity="cosine")
    tic = time.perf_counter()
    retriever = EmbeddingRetriever(
    document_store=document_store,
    embedding_model="jegormeister/bert-base-dutch-cased-snli",
    model_format="sentence_transformers",
    )
    print("Updating embeddings")

    document_store.update_embeddings(retriever,update_existing_embeddings=False)

    toc = time.perf_counter()
