# %%
#PANDAS, MATPLOTLIB
import pandas as pd 
import xml.etree.ElementTree as et
import os
import io
import re
import zipfile


# The purpose of this script is to do the following:
# 1. From a series of open data XMLs provided by the Dutch government (https://www.rechtspraak.nl/Uitspraken/paginas/open-data.aspx), fetch the legal cases, the result, and metadata
# -. The result should be a large *list* of *dictionaries* (with lists and dicts embedded) with metadata and text
# 2. This data is pushed to a number of CSV files. Due to their size, they are split by year and additionally split into the 'caseinfo' metadata files and 'casetext' pure text files
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
# Meta_2 contains items where we create a dictionary of dictionaries, to get all the formal relations
# This is coded directly into the script
# Meta_3 refers to all tags that are contained in the 'uitspraak' / conclusion element of the XML documents

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

#meta_2 = ('vindplaatsen','relatie', 'inhoudsindicatie')

meta_3 = ('{http://www.rechtspraak.nl/schema/rechtspraak-1.0}para',
               '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}conclusie',
               '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}parablock',
               '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}emphasis',
               '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}footnote-ref',
               '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}section',
               '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}title',
               '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}nr',
               '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}bridgehead',
               '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}paragroup',
               '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}footnote',
              '{http://www.rechtspraak.nl/schema/rechtspraak-1.0}uitspraak')
 
conclusie = ('{http://www.rechtspraak.nl/schema/rechtspraak-1.0}uitspraak','{http://www.rechtspraak.nl/schema/rechtspraak-1.0}conclusie')


# We also want to identify the defendants and the plaintiffs, as well as the text of the ultimate *verdict* of the case
# The below strings should help identify where these blocks with the 'uitspraak' section occur, using RegEx:
beslissing_strings = ["Beslissing","De beslissing","BESLISSING"]
beslissing_string = "(([0-9]\. )?"+")|(([0-9]\. )? ".join(beslissing_strings)+")"

players_strings = ["Uitspraak (.*?) tussen","Uitspraak (.*?) van","Procedure","Procesverloop","Inhoud",
                   "Het geding in hoger beroep","Onderzoek van de zaak", "Ontstaan en loop van het geding",
                    "Het geding in eerste aanleg", "Het verdere verloop van het geding", "Het geding voorafgaande",
                   "Het geding in feitelijke instantie", "Geding in cassatie","Conclusie (.*?) inzake"]
players_string = "("+")|(".join(players_strings)+")"

strings = ["http://deeplink.rechtspraak.nl/uitspraak\?id=","\\n"]
string = "("+")|(".join(strings)+")"

# To assist with the cleaning of references to other legal cases, we want to also scrub out references such as the below:
wetboeken = ["Wft","Bpr","Rv","EVRM","BW","Sv","WvSv","Sr","WvS","WvSr","WvK","Awb","Mw","Tw","Wtra"]


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
         }
    text={}
    refs={}
    clean_text={}

    #data2['filesize'] = os.stat(doc).st_size
    refs['uitgaande_links'] = {}
    refs['inkomende_links'] = {}
    errors = {'Tekst_missing' : []}
    #errors['Filesize'] = data2['filesize']
    xtree = et.parse(doc)
    xroot = xtree.getroot()
    print(doc)

    error = 0
    
    errors['Missing'] = []
    errors['Tekst_missing'] = 0
    errors['Parse'] = 0
    #print("-Start parsing")
    #print("--Meta information")
    for x in meta_1:
        if x == 'rechtsgebied':
            try:
                temp=[]
                for i in xroot.findall(".//" + namespaces[x]):
                    temp.append(i.text)
                meta[x] = temp
            except:
                #print("Error with rechtsgebied")
                pass
        else:
            try: 
                meta[x] = xroot.find(".//" + namespaces[x]).text
            except:
                meta[x] = "Not found"
                #print(x + "error")
                error = 1
                errors['Missing'].append(x)

    # Find all the references to a) where the case is produced, b) references , c) a previous stage of the same case,        

    for child in xroot.iter(namespaces['opsom']):
        meta['vindplaatsen'].append(child.text)

    for child in xroot.iter(namespaces['references']):
        meta['relaties'].append(child.text)

    for child in xroot.iter(namespaces['relatie']):
        meta['relaties'].append(child.text)

    meta['commentaren'] = len(meta['vindplaatsen'])

    # Capture the second part of the XML: a summary of the case
    #print("--Summary and full text")

    try:
        for child in xroot.iter(namespaces['inhoudsindicatie']):
            #print(child.text)
            meta['inhoudsindicatie'] = re.sub(string,"",''.join(child.itertext()))
            #print("is er")
    except:
        meta['inhoudsindicatie'] = " "
        #print("---No summary")

    try:
        x = xroot[2].tag
        text['tekst'] = ''.join(xroot[2].itertext())
        #print(x)
    except:
        #print("---No full text")
        error = 2
        errors['Tekst_missing'] = 1
        text['tekst'] = ""
    
    return(meta,errors,text)

# PARSE function parses the XML file


# %%
def appendcsv(zaken_rich, haystack, zaken_err, year):
    #print("-Appending to CSV")
    zaken_rich_out = pd.DataFrame.from_dict(zaken_rich, orient='index')
    zaken_rich_out.rename(columns={zaken_rich_out.columns[0]:'id'},inplace=True)
    zaken_rich_out = zaken_rich_out.iloc[:,0:]
    filename = "caseinfo_"+year +".csv"
    if not os.path.isfile(filename):
        zaken_rich_out.to_csv(filename, mode='a', header=True, encoding="utf-8",sep='|',index=False)
    else:
        zaken_rich_out.to_csv(filename, mode='a', header=False, encoding="utf-8",sep='|',index=False)
   
    textfilename = "casetext_"+year+".csv"
    haystack_out = pd.DataFrame.from_dict(haystack, orient='index')


    haystack_out.to_csv(textfilename, mode='a', header=False, encoding="utf-8",sep='|',index=False)

    #errs_out = pd.DataFrame.from_dict(zaken_err, orient='index')
    #errs_out.to_csv('caseerrors.csv', mode='a', header=False)


# %%
def rework(year):
    filename = "caseinfo_"+year +".csv"
    textfilename = "casetext_"+year+".csv"
    df1 = pd.read_csv(filename,encoding="utf-8",delimiter="|")
    df2 = pd.read_csv(textfilename,encoding="utf-8",delimiter="|",names=["tekst"])
    mapper = pd.read_csv("instanties_map.csv",encoding="utf-8")
    df1 = df1.merge(mapper,left_on='instantie',right_on='instantie',how='left')
    
    # Map DFs - rechtsgebieden
    mapper2 = pd.read_csv("rechtsgebieden_map.csv",encoding="utf-8")
    df1['rechtsgebied'] = df1['rechtsgebied'].astype(str)
    df1['rechtsgebied'] = df1['rechtsgebied'].map(lambda x: x.lstrip("\['").rstrip("'/]"))
    df1 = df1.merge(mapper2,left_on='rechtsgebied',right_on='Raw',how='left')

    df1['procedure'] = df1['procedure'].str.replace("Not found","Geen informatie")

    # Add the 'jurisprudentie' column
    df1['Bron'] = 'Jurisprudentie'

    # Add the text as the final column

    #kill some columns
    df1.drop(["publisher","bereik","rechtsgebied","vervangt","relaties","commentaren","Source","Raw"],axis=1,inplace=True)
    df1['titel'][df1['procedure']=="Geen informatie"] = df1['id'] + ": zaak " + df1['zaaknummer'] +" van " + df1['datum'] + " bij de " + df1['instantie']
    df1['titel'][df1['procedure']!="Geen informatie"] = df1['id'] + ": zaak " + df1['zaaknummer'] +" van " + df1['datum'] + " bij de " + df1['instantie'] +". " + df1['procedure']

    df1['text'] = df2['tekst'].to_numpy()

    df1.drop_duplicates(inplace=True)
    df1 = df1[df1['text'].notna()]
    outfile = "caseinfopush_"+year+".csv"
    print("Prepared updated df file")
    df1.to_csv(outfile,mode='w+', header=True, encoding="utf-8",sep='|')
    print("Wrote updated df file")
    del df1
    del df2
    print("Dropped old df")
    
    

# %%
errs=[]
# enter the folder where you store the OpenDataUitspraken file
path_source_eclis = ''
dirs = os.listdir(path_source_eclis)
for i in dirs[16:]:
    subdirs = os.listdir(path_source_eclis+"\\"+i)
    for k in subdirs:
        #print(k)
        try:
            z = zipfile.ZipFile(path_source_eclis+"\\"+i + "\\" + k)
            for j in z.infolist():
                if j.filename.endswith(".xml"):
                    #print("Capturing information for:" + j.filename +", size is " + str(j.file_size))
                    zaken_rich={}
                    zaken_err={}
                    refs_in={}
                    refs_out={}
                    haystack={}
                    model_step1={}
                    f = z.open(j)
                    parsed = parse(f)
                    temp_zaken = parsed[0]
                    errors = parsed[1]
                    temp_zaken['filesize'] = j.file_size
                    errors['filesize'] = j.file_size
                    caseid = temp_zaken['identifier']
                    if temp_zaken['identifier']:

                        zaken_rich[caseid] = temp_zaken
                        haystack[caseid] = parsed[2]['tekst']

                        #identifier = refs_in[caseid]

                    if errors:
                        zaken_err[caseid] = errors


                    appendcsv(zaken_rich, haystack, zaken_err, i)
        except Exception as e:
            print(e)
            errs.append(e)
    rework(i)
