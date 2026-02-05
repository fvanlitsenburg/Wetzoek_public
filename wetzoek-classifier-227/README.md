# Wetzoek Category Classifier

Classification system for dutch law search queries (used by wetzoek project).

## Requirements
* python
* pip
* virtualenv

## Setup instructions
```sh
virtualenv env --python=python3.9
source env/bin/activate
pip install -r requirements.txt
```

## Usage (for testing)

The application requires three files to be loaded on startup:
* `./sample_data/dutch.txt`: required for clean up of input text 
* `./sample_data/model_20211127.pkl`: predictive model
* `./sample_data/law_titles.csv`: required for human readable output of the model

Before running the `run.py` file ensure that the paths to these configurations are set up correctly.  
The queries to be tested are set up in the array in the `run.py` file as well.

```sh
python run.py
```
## Run local server
This is an example of env variables, please copy those into an .env file:
```sh
DUTCH_TXT_PATH='sample_data/dutch.txt'
MODEL_PKL_PATH='sample_data/model_20211127.pkl'
LAW_TITLES_CSV_PATH='sample_data/law_titles.csv'
```
To run the local server:
```sh
uvicorn api:app --reload
```

Then go to check the apis: 

```sh
http://127.0.0.1:8000/docs
```

## Module usage

Import and instantiate the `ModelProvider` class from the query_classifier module like done in `run.py`.
Call the `predict` method to get an array of predictions formatted for HTML output.


## Code style

Set up your editor to use `black` and 4 spaces as the default python formatter / settings.

## Deployment instructions
### Build the docker image
```sh
docker build -t wetzoek-classifier:main .
```

### Run the application
```sh
# Run interactively on port 5000
docker run --rm -it -p 5000:80 wetzoek-classifier:main

# Run detached on port 7099
docker run -p 7099:80 -d wetzoek-classifier:main
```
