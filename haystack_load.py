import logging
import subprocess
import time
import json
import os

from haystack.document_stores import ElasticsearchDocumentStore
from haystack.utils import launch_es
import pandas as pd
from pathlib import Path
from haystack.nodes import PreProcessor
import numpy as np
import dateutil
import datetime
from datetime import date

MAPPING = {
    # Shared by cases and laws
    "id": "id",
    "titel": "title",
    "text": "content",
    "datum": "date",
    "Bron": "documentType",

    # Case specific
    "inhoudsindicatie": "summary",
    "issued": "issued",
    "instantie": "judicialAuthority",
    "zaaknummer": "caseNumber",
    "type": "caseType",
    "procedure": "procedure",
    "vindplaatsen": "journalComments",
    "filesize": "fileSize",
}

logger = logging.getLogger(__name__)

doc_index = "document"
language = "dutch"
n_docs = 2000000
doc_dir = Path("~/data")
split_length = 100

files = [
'caseinfopush_1991.csv',
'caseinfopush_1992.csv',
'caseinfopush_1993.csv',
'caseinfopush_1994.csv',
'caseinfopush_1995.csv',
'caseinfopush_1996.csv',
'caseinfopush_1997.csv',
'caseinfopush_1998.csv',
'caseinfopush_1999.csv',
'caseinfopush_2000.csv',
'caseinfopush_2001.csv',
'caseinfopush_2002.csv',
'caseinfopush_2003.csv',
'caseinfopush_2004.csv',
'caseinfopush_2005.csv',
'caseinfopush_2006.csv',
'caseinfopush_2007.csv',
'caseinfopush_2008.csv',
'caseinfopush_2009.csv',
'caseinfopush_2010.csv',
'caseinfopush_2011.csv',
'caseinfopush_2012.csv',
'caseinfopush_2013.csv',
'caseinfopush_2014.csv',
'caseinfopush_2015.csv',
'caseinfopush_2016.csv',
'caseinfopush_2017.csv',
'caseinfopush_2018.csv',
'caseinfopush_2019.csv',
'caseinfopush_2020.csv',
'caseinfopush_2021.csv',
]

def cases_to_dicts(filename_cases,type='Cases'):
    ret = []
    df = pd.read_csv(doc_dir / filename_cases, nrows=n_docs, sep="|")
    df = df.reset_index(drop=True)
    df = df.replace({np.nan: None})

    df = df.rename(columns=MAPPING)
    df["date"] = df["date"].apply(dateutil.parser.parse)
    records = df.to_dict(orient="records")
    for i, r in enumerate(records):
        text = r.pop("content")
        id = r.pop("id")
        print("Printing name")
        r["name"] = str(i)
        ret.append(
            {"content": text, "id":id,  "meta": r}
        )

    return ret

def main(filename_cases,type_fn):
    launch_es()
    document_store = ElasticsearchDocumentStore(analyzer=language, index=doc_index, timeout=300)
    print("Reading file...")

    dicts = cases_to_dicts(filename_cases,type_fn)

    print()

    print("Number of docs before splitting")
    print(len(dicts))
    print()
    preprocessor = PreProcessor(split_length=split_length)
    tic = time.perf_counter()

    print("Preprocessing...")
    docs = preprocessor.process(documents=dicts)
    toc = time.perf_counter()

    print(toc-tic)

    print("Number of split docs")
    print(len(docs))

    tic = time.perf_counter()
    print("Writing to document store...")

    document_store.write_documents(dicts, index=doc_index,duplicate_documents='overwrite')

    toc = time.perf_counter()

    print(toc-tic)

if __name__ == "__main__":
    for i in files:
        print(i)
        main(i,'Cases')

