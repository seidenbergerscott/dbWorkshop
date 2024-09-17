# dbWorkshop

## Steps to Use this Repo

1. Clone this GitHub repo
2. Download, unzip and put the `USCensus1990.data.txt` in the main directory of this repo. `https://archive.ics.uci.edu/dataset/116/us+census+data+1990`
3. Install Docker and deploy the databases
   1. Make sure that docker is installed on your system. `https://docs.docker.com/engine/install/`
   2. Deploy the databases by running `docker compose up -d`
   3. Verify the database containers are running `docker ps`
4. Make a Python virtual environment (recommended)
   1. `python -m venv`
   2. Install the dependencies of this repo `pip install -r requirements.txt`
5. Populate the databases with the sample data
   1. Run `ingestion_test.py` and `census_ingest_mongo.py` to load in the Census dataset
   2. Run `ecommerce_ingest.py` and `ecommerce_ingest_postgres.py` to load the ecommerce dataset into each database
6. Run the streamlit application `streamlit run app.py`
   1. streamlit will list the URLs of where to reach the app
   2. The default is to `localhost:8501` and also on your local subnet
7. Populate the test data to see the differences in database performance via `python populate_query_logs.py`
8. Evaluate results!

## Individual Tests

In this repo are also a series of individual performance tests that you can run. For example, to see the improvements of indexing a SQL database, you can run `python indexing_test.py`. Some of my results are in the `results.md` document.

## Powerpoint

The presentation of this workshop is available as a PowerPoint and is available as part of this repo, it gives a general motivation for understanding your data flow and database selection. Database selection is an active choice that needs to be made by the discerning data scientist. Understanding the actual mechanics of data storage, transport, and processing is a key piece of education that is missed in data science programs.

## Links and Notes

- A good overview of database systems and where I got some images: `https://cs186berkeley.net/notes/note17/`