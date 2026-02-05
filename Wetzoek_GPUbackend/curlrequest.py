import requests

headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json',
}

reqs = ['Wat kost een boerderij?',
'Noodweer',
'Onder welke omstandigheden is er sprake van handelen met voorkennis?',
'Is witwassen toegestaan?',
'Aansprakelijkheid',
'Wat is Privacy by Design en wat heeft het te maken met cyberaanvallen?',
'Wat als turboliquidatie onterecht is toegepast omdat alsnog van een bate blijkt?',
'Aanbesteding softwareprogramma, toch maar onderhands?',
'Faillissement; wat betekent dat voor de bestuurder?',
'Welke looncomponenten tellen mee voor de berekening van de transitievergoeding?',
'Status advies Commissie van Aanbestedingsexperts?',
'Toe- of uittreding van aandeelhouders?',
'Moet ik als bestuurder van een onderneming deze informatie met de aandeelhouder delen?',
'Wat te doen als de ondernemer van de coronasteun een zwembad heeft laten aanleggen?',
'Is de beste prijs garantie op boekingssites mededingingsrechtproof?',
'Kan een huurder aanspraak maken op verjaring?',
'Modernere (kortere) partneralimentatie?',
'kortere partneralimentatie',
'Verhoging fiscale pensioenrichtleeftijd: wat betekent dit voor pensioenleeftijd in pensioenreglement? ',
]

for j in reqs:

    json_data = {
        'query': j,
        'params': {},
        'debug': False,
    }

    response = requests.post('http://localhost:8000/query', headers=headers, json=json_data)
    #print(response.content)