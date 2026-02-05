import logging
import subprocess
import time
import json
import os

from haystack.document_stores import ElasticsearchDocumentStore
from haystack.document_stores import WeaviateDocumentStore

from haystack.nodes import EmbeddingRetriever

from haystack.utils import launch_es
import pandas as pd
from pathlib import Path
from haystack.nodes import PreProcessor
import numpy as np
import dateutil
import datetime
import pyrfc3339
import pytz
from datetime import date
import traceback

MAPPING = {
    # Shared by cases and laws
    "id": "id",
    "titel": "title",
    "text": "content",
    "datum": "date",
    "Level1": "areaLevel1",
    "Level2": "areaLevel2",
    "Level3": "areaLevel3",
    "BES": "BES_eilanden",
    "Bron": "documentType",
    "added":"added",

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

cols_allow = [
 # Shared by cases and laws
 "id",
 "title",
 "content",
 "date",
 "areaLevel1",
 "areaLevel2",
 "areaLevel3",
 #"BES_eilanden",
 "documentType",
 "added",

 "summary",
 "issued",
 "judicialAuthorityLevel3",
 "caseNumber",
 "caseType",
 "procedureLevel1",
 "procedure",
 "journalComments",
 "fileSize",
 "judicialAuthorityLevel1",
 "judicialAuthorityLevel2",


 "chapter",
 "article",
 "articleDate",
 "responsibleMinistry",
 "inEffect",
 "lawType"
]

logger = logging.getLogger(__name__)

doc_index = "document"
split_doc_index = "splitdoc"
language = "dutch"
n_docs = 2000000
batch_size = 5
doc_dir = Path("~/data")
split_length = 100

embedding_model="jegormeister/bert-base-dutch-cased-snli"
model_format="sentence_transformers"



pre_embed = False # specify if Documents should be embedded *beforehand* or afterwards

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

dates = ['2021-10','2021-11','2021-12','2022-01','2022-02','2022-03','2022-1','2022-2','2022-3','2022-04','2022-05','2022-06','2022-07','2022-08','2022-09','2022-10']

lawfiles = ['20210728+laws_mvp_clean.csv']

def split_dataframe(df, chunk_size = 250): 
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i*chunk_size:(i+1)*chunk_size])
    return chunks

def cases_to_dicts(filename_cases):
    '''
    Convert CSV of cases to dictionaries, then load them into Haystack
    '''
    df = pd.read_csv(doc_dir / filename_cases, nrows=n_docs, sep="|")
    
    df = df.drop('Unnamed: 0',axis=1)
    df = df.rename(columns=MAPPING)

    # Split dataframe into chunks so machine doesn't crash
    split_df = split_dataframe(df)
    count = 0
    for j in split_df:
        print(count," / ",len(split_df))
        count += 1
        load(convert_df(j))

def laws_to_dicts(filename):
    ''' Convert CSV of laws to dictionaries, then load into Haystack
    '''
    df = pd.read_csv(doc_dir / filename, nrows=n_docs, index_col=0)
    df = df.drop('Unnamed: 0',axis=1)
    df = df.rename(columns=MAPPING)
    df["articleDate"] = df["articleDate"].str.replace("/","-") # replace /s with -s to ensure uniform format
    

    # Split dataframe into chunks so machine doesn't crash
    split_df = split_dataframe(df)
    count = 0
    for j in split_df:
        print(count," / ",len(split_df))
        count += 1
        load(convert_df(j))

def convert_df(df):
    '''
    Convert a dataframe into a list of dictionaries for Haystack.

    The dataframe must have the following fields:
    --> content
    --> id

    All other fields will go into the document store in 'meta' and must be in the allowlist

    '''
    ret = []
    df = df
    df = df.reset_index(drop=True)
    df = df.fillna(" - ")
    df = df.rename(columns=MAPPING)

    df['added'] = datetime.datetime.today().strftime('%Y-%m-%d')

    # Only write to DocumentStore what's in the allowlist
    df = df[df.columns.intersection(cols_allow)]

    # Add "Empty" columns for the cols_allow columns that are not already included


    records = df.to_dict(orient="records")
    for i, r in enumerate(records):
        text = r.pop("content")
        ids = r.pop("id")

        # We add the id back in to meta
        r['code'] = str(ids)
        if text is not None:
            ret.append(
                {"content": text, "id":ids, "meta": r}
            )
            
        else:
            pass

    return ret

def embed():

    print("Adding embeddings")

    tic = time.perf_counter()

    retriever = EmbeddingRetriever(
    document_store=split_document_store,
    embedding_model=embedding_model,
    model_format=model_format,
    )
    print("Updating embeddings")

    split_document_store.update_embeddings(retriever,update_existing_embeddings=False)

    toc = time.perf_counter()
    
    print(toc-tic)

def pre_embedder(docs):
    print('Running the pre-embedding')
    retriever = EmbeddingRetriever(
    document_store=split_document_store,
    embedding_model=embedding_model,
    model_format=model_format,
    )
    embeds = retriever.embed_documents(docs)
    for doc, emb in zip(docs,embeds):
        try:
            doc.embedding = emb
        except Exception as e:
            print(e)
    return docs

def load(ret):
    '''
    Load a list of dictionaries in Haystack format into the DocumentStore. 
    '''
    
    print('Loading document stores')
    global document_store
    global split_document_store
    
    document_store = ElasticsearchDocumentStore(analyzer=language, index=doc_index, timeout=300)

    if pre_embed is False:
        split_document_store = ElasticsearchDocumentStore(index=split_doc_index,similarity='cosine')
    else:
        split_document_store = WeaviateDocumentStore(index=split_doc_index,similarity='cosine',timeout_config=(5,120))
        print('Running with pre-embedding')

    dicts = ret
    print("Number of docs before splitting")
    print(len(dicts))
    print()

    # Preprocessing
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
    print(pre_embed)
    if pre_embed == True:
        print('About to run the pre-embedding')
        try:
            docs = pre_embedder(docs)
        except Exception as e:
            print(e)

    # Write the documents to the DocumentStore. Note: there are two document stores, one for the full documents (for ES search and as database).
    # The other document store is for embedded documents and semantic search
    try:
        print("Writing split documents")
        split_document_store.write_documents(docs, index=split_doc_index,duplicate_documents='overwrite',batch_size=batch_size)
        print("Writing documents")
        document_store.write_documents(dicts, index=doc_index,duplicate_documents='overwrite')
    except Exception as e:
            print(e)
    
    toc = time.perf_counter()

    print(toc-tic)
    
    if pre_embed == False:
        embed()

def main_fetched(filename_cases):

    df1 = pd.read_csv(filename_cases,sep="|")
    print(df1)
    mapper = pd.read_csv("~/Wetzoek_GPUbackend/instanties_map.csv",encoding="utf-8")
    mapper2 = pd.read_csv("~/Wetzoek_GPUbackend/rechtsgebieden_map.csv",encoding="utf-8")
    mapper3 = pd.read_csv("~/Wetzoek_GPUbackend/procedure_map.csv",encoding="utf-8")

    try: 
        # Map DFs - instanties

        df1 = df1[df1['text'].notna()]

        if len(df1) > 0:
        
            df1 = df1.merge(mapper,left_on='instantie',right_on='instantie',how='left')
            df1 = pd.merge(df1,mapper3,how='left',left_on='procedure',right_on='procedure')
            
            # Map DFs - rechtsgebieden
            
            df1['rechtsgebied'] = df1['rechtsgebied'].astype(str)
            df1['rechtsgebied'] = df1['rechtsgebied'].map(lambda x: x.lstrip("\['").rstrip("'/]"))
            df1 = df1.merge(mapper2,left_on='rechtsgebied',right_on='Raw',how='left')

            # Add the 'jurisprudentie' column
            df1['Bron'] = 'Jurisprudentie'

            #kill some columns
            df1.drop(["publisher","bereik","rechtsgebied","vervangt","relaties","commentaren","Source","Raw"],axis=1,inplace=True)
            try:
                df1['titel'][(df1['procedure']=="Geen informatie")|(df1['procedure']=="Not found")] = df1['id'] + ": zaak " + df1['zaaknummer'] +" van " + df1['datum'] + " bij de " + df1['instantie']
                df1['titel'][(df1['procedure']!="Geen informatie")&(df1['procedure']!="Not found")] = df1['id'] + ": zaak " + df1['zaaknummer'] +" van " + df1['datum'] + " bij de " + df1['instantie'] +". " + df1['procedure']
            except:
                df1['titel'] = df1['id']
            print("Prepared updated df file")
            print(df1['titel'])
            print(df1['procedure'])
            try:
                load(convert_df(df1))
                print("Added to Haystack")
                
            except:
                e = traceback.format_exc()
                #errlog(e)
                print("Error indexing doc - written to log")
                print(e)
        else:
            print("No DF left after removing empty text")
    except Exception as e:
        print(e)

def main(data_in,type_fn='Cases'):

    print("Reading file...")

    if type_fn == 'Cases':
        print('Cases from data loaded')
        cases_to_dicts(data_in)
    if type_fn == 'Update':
        print('Old cases from chronjob')
        main_fetched(data_in)
    if type_fn == 'Laws':
        print('Laws from data')
        laws_to_dicts(data_in)

if __name__ == "__main__":

    print("Starting!")
    count = 0
    count_len = 0
    for i in files[20:21]:
        print(i)

        fetchfile = i[0:-4] + "b.csv"
        main(fetchfile,'Cases')
        #test = pd.read_csv(doc_dir / fetchfile, nrows=n_docs, sep="|")
    '''
    for i in lawfiles:
        print(i)
        main(i,'Laws')
    dirs = next(os.walk("../data"))[1]
    for i in dirs:
        for j in next(os.walk("../data/" + i))[2]:
            if j.endswith(".csv"):
                fetchfile = "../data/" + i + "/" + j
                print(fetchfile)
                main(fetchfile,'Update')
    print("Done")'''