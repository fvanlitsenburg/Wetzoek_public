import logging
import subprocess
import time
import json
import os

from haystack.document_stores import ElasticsearchDocumentStore
from haystack.utils import launch_es
import pandas as pd
from pathlib import Path
import numpy as np
import dateutil

MAPPING = {
    # Shared by cases and laws
    "id": "id",
    "titel": "title",
    "text": "content",
    "datum": "date",
    "Level1": "areaLevel1",
    "Level2": "areaLevel2",
    "Level3": "areaLevel3",
    "BES": "BES",
    "Bron": "documentType",

    # Case specific
    "inhoudsindicatie": "summary",
    "issued": "issued",
    "instantie": "judicialAuthorityLevel3",
    "zaaknummer": "caseNumber",
    "type": "caseType",
    "procedureLevel1": "procedureLevel1",
    "procedure": "procedure",
    "vindplaatsen": "journalComments",
    "filesize": "fileSize",
    "Instantie_Level1": "judicialAuthorityLevel1",
    "Instantie_Level2": "judicialAuthorityLevel2",

    # Law specific
    "Hoofdstuk": "chapter",
    "Artikel": "article",
    "Artikel-datum": "articleDate",
    "Eerstverantwoordelijke": "responsibleMinistry",
    "Geldigheid": "inEffect",
    "WettenCAT": "lawType"
}

logger = logging.getLogger(__name__)

doc_index = "document"
language = "dutch"
n_docs = 9000000
doc_dir = Path("data")
split_length = 100

def main():

    document_store = ElasticsearchDocumentStore(analyzer=language, index=doc_index, timeout=300)
    test = document_store.get_metadata_values_by_key("title")
    print(test)

if __name__ == "__main__":
    main()
