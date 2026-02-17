# Wetzoek

Repository for [Wetzoek](https://www.wetzoek.nl/), a project to make Dutch law more accessible.

This project was made possible thanks to the [SIDN Fonds](https://www.sidnfonds.nl/). It has been carried out primarily by Felix van Litsenburg, with assistance from [Branden Chan](https://github.com/brandenchan), [Elena Stein](https://github.com/elenche95), and Julia van Litsenburg.

Wetzoek uses [Haystack](https://haystack.deepset.ai/) for retrieval + QA, with supporting services for a web UI and query classification.

## Repository layout

- **`parser.py`**: Parses Rechtspraak.nl open data XML dumps into CSV exports (metadata + text).
- **`haystack_load.py`**: Loads CSV exports into an Elasticsearch-backed Haystack `DocumentStore` (with preprocessing/splitting).
- **`pipelines.yaml`**: Example Haystack pipeline config (Retriever + FARMReader).
- **`Wetzoek_GPUbackend/`**: Utilities/scripts for embedding + (GPU) ingestion/query workflows (Haystack + Elasticsearch).
- **`wetzoek-classifier-227/`**: FastAPI service that classifies search queries into legal categories.
- **`wetzoek-app-227/`**: Next.js web app (UI) that queries the Haystack backend and Elasticsearch, and optionally the classifier.

## Running locally (high level)

### Frontend (`wetzoek-app-227/`)

```bash
cd wetzoek-app-227
npm install
cp .env.example .env
npm run dev
```

The app expects the following environment variables (see `.env.example`):

- `ES_URI`: Elasticsearch endpoint (used by the Next.js API routes).
- `HS_URI`: Haystack API endpoint (used for `/query` and `/feedback`).
- `CLASSIFIER_URI`: Classifier API endpoint (optional).
- `HS_SCORE_THRESHOLD`: Score threshold for filtering Haystack answers.

### Query classifier API (`wetzoek-classifier-227/`)

```bash
cd wetzoek-classifier-227
python -m venv env
source env/bin/activate
pip install -r requirements.txt
uvicorn api:app --reload
```

The classifier loads model/data files on startup. You can override paths via environment variables (see `wetzoek-classifier-227/README.md`).

#### Downloading the classifier model (not stored in this repo)

The default API configuration (`wetzoek-classifier-227/api.py`) expects a model at `sample_data/20230125model.pkl`. The model binary is intentionally **not** committed to this public repository.

- **Download link**: [20230125model.pkl](https://uitspraken.s3.eu-central-1.amazonaws.com/20230125model.pkl)

From the repo root:

```bash
mkdir -p wetzoek-classifier-227/sample_data
curl -L "https://uitspraken.s3.eu-central-1.amazonaws.com/20230125model.pkl" \
  -o wetzoek-classifier-227/sample_data/20230125model.pkl
```

Alternatively, set `MODEL_PKL_PATH` to wherever you placed the file before starting the server.



## Notes

- The frontend work was originally developed by [Berlin Bytes](https://berlin-byt.es/en/). If you’re making this repository public, ensure you’re comfortable with the licensing/redistribution status of all included code and assets.
