import pandas as pd 
import xml.etree.ElementTree as et
import os
import io
import re
import requests
from datetime import timedelta, date
from bs4 import BeautifulSoup
from pathlib import Path
import traceback
import time

# The purpose of this script is to do the following:
# 1. From a series of open data XMLs provided by the Dutch government, fetch the legal cases, the result, and metadata
# -. The result should be a large *list* of *dictionaries* (with lists and dicts embedded) with metadata and text
# 2. This data will be added to the ElasticSearch instance
# 3. The output appends to a number of CSV files the key data captured from the open data

### SETUP ###

# The namespaces dictionary contains the correct -tags- for key metadata in XMLs, automatically adding the relevant
# namespaces info

namespaces = {'identifier' :  '{http://purl.org/dc/terms/}identifier',
               'issued' :  '{http://purl.org/dc/terms/}issued',
               'publisher' :  '{http://purl.org/dc/terms/}publisher',
               'instantie' :  '{http://purl.org/dc/terms/}creator',
               'datum' :  '{http://purl.org/dc/terms/}date',
               'zaaknummer' :  '{http://psi.rechtspraak.nl/}zaaknummer',
                'procedure' :  '{http://psi.rechtspraak.nl/}procedure',
              'vervangt' :  '{http://purl.org/dc/terms/}replaces',
              'relatie' :  '{http://purl.org/dc/terms/}relation',
              'references' : '{http://purl.org/dc/terms/}references',
               'type' :  '{http://purl.org/dc/terms/}type',
               'bereik' :  '{http://purl.org/dc/terms/}coverage',
               'rechtsgebied' :  '{http://purl.org/dc/terms/}subject',
               'vindplaatsen' :  '{http://purl.org/dc/terms/}hasVersion',
               'vindplaatslijst' :  '{http://www.w3.org/1999/02/22-rdf-syntax-ns#}list',
              'opsom' : '{http://www.w3.org/1999/02/22-rdf-syntax-ns#}li',
               'inhoudsindicatie' :  '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}inhoudsindicatie',
               'para' :  '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}para',
               'conclusie' :  '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}conclusie',
               'parablock' :  '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}parablock',
               'nadruk' :  '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}emphasis',
               'voetnootref' :  '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}footnote-ref',
               'sectie' :  '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}section',
               'titel' :  '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}title',
               'nr' :  '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}nr',
               'kopje' :  '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}bridgehead',
               'subsectie' :  '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}paragroup',
               'voetnoot' :  '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}footnote',
              'uitspraak' : '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}uitspraak'
}

# Based on the structure of the XML, we split it into different elements:
# Meta_1 contains all elements where we just want to take the first text instance

meta_1 = ('identifier',
'issued',
'publisher',
'instantie',
'datum',
'zaaknummer',
'type',
'bereik',
'rechtsgebied',
'procedure',
'vervangt',
'rechtsgebied',
'titel'
)

# %%
# PARSE function parses the XML file

def parse(doc):
    meta={'identifier':'',
        'issued':'',
        'publisher':'',
        'instantie':'',
        'datum':'',
        'zaaknummer':'',
        'type':'',
        'bereik':'',
        'rechtsgebied':'',
        'procedure':'',
        'vervangt':'',
        'vindplaatsen':[],
        'relaties':[],
        'inhoudsindicatie':'',
        'rechtsgebied':'',
        'commentaren':'',
        'titel':'',
        'filesize':'',
         }
    text={}

    xroot = et.fromstring(requests.get(doc).text)
    print(doc)

    print("-Start parsing")
    print("--Meta information")
    # Cycle through the items in meta_1 which are plain text and can be directly added
    for x in meta_1:
        if x == 'rechtsgebied':
            try:
                temp=[]
                for i in xroot.findall(".//" + namespaces[x]):
                    temp.append(i.text)
                meta[x] = temp
            except:
                print("Error with rechtsgebied")
        else:
            try: 
                meta[x] = xroot.find(".//" + namespaces[x]).text
            except:
                meta[x] = "Not found"
                print(x + " error")

    # The 'vindplaatsen' and 'relaties' are lists, so we approach these slightly differently:

    for child in xroot.iter(namespaces['opsom']):
        meta['vindplaatsen'].append(child.text)

    for child in xroot.iter(namespaces['references']):
        meta['relaties'].append(child.text)

    for child in xroot.iter(namespaces['relatie']):
        meta['relaties'].append(child.text)

    # The number of commentaries is the length of vindplaatsen

    meta['commentaren'] = len(meta['vindplaatsen'])

    # Capture the second part of the XML: a summary of the case
    print("--Summary and full text")

    try:
        for child in xroot.iter(namespaces['inhoudsindicatie']):
            #print(child.text)
            meta['inhoudsindicatie'] = ''.join(child.itertext())
            print("---Summary included")
        errlog("- Summary captured")
    except:
        meta['inhoudsindicatie'] = " "
        errlog("- Summary not captured or found")
        print("---No summary")

    # Capture the full text of the case:

    try:
        x = xroot[2].tag
        text['tekst'] = ''.join(xroot[2].itertext())
        errlog("- Full text captured")
        print("---Full text included")
    except:
        errlog("- Full text not captured or found")
        print("---No full text")
        text['tekst'] = ""
    
    meta['filesize'] = len(text['tekst'])

    return(meta,text)
    

# PARSE function parses the XML file


# %%
def appendcsv(df_in,id_in):
    print("-Appending to CSV")

    # create path to append the CSV

    id_out = re.sub(":","_",str(id_in))

    location = id_out + ".csv"

    csv_location = os.path.join(final_directory,location)

    if not os.path.isfile(csv_location):
        df_in.to_csv(csv_location, mode='a+', header=True, encoding="utf-8",sep='|',index=False)
    else:
        df_in.to_csv(csv_location, mode='a', header=False, encoding="utf-8",sep='|',index=False)

    errlog("- saved csv")


# %%
def errlog(errtext):
    errtext = "\n" + errtext
    o = os.path.join(final_directory, "log.txt")
    file = open(o, "a+")
    file.write(errtext)
    file.close()

# %%
def work_dicts(dicts):
    
    # Create DFs
    df1_dict = {}
    df1_dict['ECLI'] = dicts[0]
    df1 = pd.DataFrame.from_dict(df1_dict, orient='index')
    text = {}
    text['ECLI'] = dicts[1]
    df2 = pd.DataFrame.from_dict(text, orient='index')

    # prepare the DF to save as CSV

    df1.rename(columns={df1.columns[0]:'id'},inplace=True)
    df_out = df1
    df_out['text'] = df2['tekst'].to_numpy()

    try:
        appendcsv(df_out,dicts[0]['identifier'])
        print("Saved CSV file")
    
    except:
        errlog("- Error saving CSV:")
        e = traceback.format_exc()
        errlog(e)
        print("Error saving CSV")

    # prepare the DF to index

    if df_out.loc[df_out.index[0],'text'] == "":
        errlog("- No text, not indexing")
        print("No text, not indexing.")
    else: 
        try: 
            # Map DFs - instanties
            
            df1 = df1.merge(mapper,left_on='instantie',right_on='instantie',how='left')
            df1 = pd.merge(df1,mapper3,how='left',left_on='procedure',right_on='procedure')
            
            # Map DFs - rechtsgebieden
            
            df1['rechtsgebied'] = df1['rechtsgebied'].astype(str)
            df1['rechtsgebied'] = df1['rechtsgebied'].map(lambda x: x.lstrip("\['").rstrip("'/]"))
            df1 = df1.merge(mapper2,left_on='rechtsgebied',right_on='Raw',how='left')

            # Add the 'jurisprudentie' column
            df1['Bron'] = 'Jurisprudentie'

            # Add the data
            df1['added'] = copydate
            df1['orig_issued'] = df1['issued']
            df1['issued'] = fetch_date

            # Add the text as the final column
            df1['text'] = df2['tekst'].to_numpy()

            #kill some columns
            df1.drop(["publisher","bereik","rechtsgebied","vervangt","relaties","commentaren","Source","Raw"],axis=1,inplace=True)
            try:
                df1['titel'][df1['procedure']=="Geen informatie"] = df1['id'] + ": zaak " + df1['zaaknummer'] +" van " + df1['datum'] + " bij de " + df1['instantie']
                df1['titel'][(df1['procedure']!="Geen informatie")&(df1['procedure']!="Not found")] = df1['id'] + ": zaak " + df1['zaaknummer'] +" van " + df1['datum'] + " bij de " + df1['instantie'] +". " + df1['procedure']
            except:
                df1['titel'] = df1['id']
            print("Prepared updated df file")

        except:
            errlog("- Error preparing indexing DF")
            e = traceback.format_exc()
            errlog(e)
            print("Error preparing indexing doc - written to log")

        try:
            from gpubuild import load, convert_df
            print(df1)
            print("Indexing")
            load(convert_df(df1))
            errlog("- Added to Haystack")

        except:
            errlog("- Error adding to Haystack")
            e = traceback.format_exc()
            errlog(e)
            print("Error indexing - written to log")

# %%
def get_parser(cases):
    urlfront = "https://data.rechtspraak.nl/uitspraken/content?id="
    global mapper, mapper2, mapper3
    mapper = pd.read_csv("~/Wetzoek_GPUbackend/instanties_map.csv",encoding="utf-8")
    mapper2 = pd.read_csv("~/Wetzoek_GPUbackend/rechtsgebieden_map.csv",encoding="utf-8")
    mapper3 = pd.read_csv("~/Wetzoek_GPUbackend/procedure_map.csv",encoding="utf-8")
    for i in cases:
        url = urlfront + i
        print(url)
        global dicts
        errlog(i)
        errlog(url)
        dicts = parse(url)
        errlog("- parsing complete")
        work_dicts(dicts)

# %%
def get_meta(today):
    global fetch_date
    fetch_date = today # for the directory, use the date *as stored in public repository*


    global copydate

    copydate = str(date.today()) # for the day of copying, use the *actual* day

    # make a new directory for this date
    current_directory = Path("/home/ubuntu/")
    global final_directory
    final_directory = os.path.join(current_directory,"data/" + str(today))
    if not os.path.exists(final_directory):
        os.makedirs(final_directory)

    # fetch the URL for meta-data on what cases were released that day
    url = "http://data.rechtspraak.nl/uitspraken/zoeken?modified="+str(today)
    page = requests.get(url,timeout=120)
    contents = BeautifulSoup(page.content, 'html.parser')

    # fetch the header info about number of cases found
    number = contents.find("subtitle")
    
    # find the ids of all the cases
    cases = contents.find_all("id")

    cases_fetch = []
    
    # cycle through the ids
    for case in cases:
        cases_fetch.append(case.text)
    cases_fetch.pop(0)

    # create log file
    loginit = "Data fetched on "+str(today)+", with "+str(len(cases_fetch))+ " / " + number.text +" cases. \n"
    errlog(loginit)
    errlog(url)

    get_parser(cases_fetch)
# %%

dates = ['2022-08-09','2022-08-10','2022-08-11','2022-08-12','2022-08-13','2022-08-14','2022-08-15','2022-08-16',
]

def daterange(date1, date2):
    for n in range(int ((date2 - date1).days)+1):
        yield date1 + timedelta(n)

if __name__ == "__main__":
    #get_meta(str(date.today()))
    #for i in dates:
    #   copydate = i
    #   get_meta(i)
<<<<<<< HEAD
    start_dt = date(2022, 6, 23)
    end_dt = date(2022, 6, 23)
    for dt in daterange(start_dt, end_dt):
        print(dt.strftime("%Y-%m-%d"))
        get_meta(dt.strftime("%Y-%m-%d"))
=======
    start_dt = date(2022, 6, 29)
    end_dt = date(2022, 10, 16)
    for dt in daterange(start_dt, end_dt):
        print(dt.strftime("%Y-%m-%d"))
        get_meta(dt.strftime("%Y-%m-%d"))
        time.sleep(5)
>>>>>>> main
