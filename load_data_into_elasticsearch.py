import csv
import json
from os.path import abspath, join, dirname, exists
import tqdm
import urllib3
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

#----------------------------------------------------------------------------------------------------
# GLOBAL VARIABLES
#----------------------------------------------------------------------------------------------------

# Source file to upload to Elasticsearch
FILE_PATH = "./liquibase-logs-1.json"

# Elastic cloud parameters
ELASTIC_PASSWORD = "*****"
CLOUD_ID = "*****"
CHUNK_SIZE = 500
INDEX_NAME = "liquibase.logs.1"

#----------------------------------------------------------------------------------------------------
# FUNCTION DEFINITIONS
#----------------------------------------------------------------------------------------------------

def data_size():
    with open(FILE_PATH) as f:
        return sum([1 for _ in f]) - 1

def generate_actions():
    count = 0
    with open(FILE_PATH, 'r') as f:
        for line in f:
            doc = json.loads(line)
            yield doc

#----------------------------------------------------------------------------------------------------
# MAIN LOGIC
#----------------------------------------------------------------------------------------------------

def main():
    print("Loading dataset...")
    number_of_docs = data_size()

    es = Elasticsearch(
        cloud_id=CLOUD_ID,
        basic_auth=("elastic", ELASTIC_PASSWORD)
    )
    print("Creating an index...")
    es.indices.delete(index=INDEX_NAME, ignore=[400, 404])
    es.indices.create(index=INDEX_NAME, ignore=400)

    print("Indexing documents...")
    progress = tqdm.tqdm(unit="docs", total=number_of_docs)
    successes = 0
    for ok, action in streaming_bulk(
        client=es, index=INDEX_NAME, actions=generate_actions(), chunk_size=CHUNK_SIZE,
    ):
        progress.update(1)
        successes += ok
    print("Indexed %d/%d documents" % (successes, number_of_docs))


if __name__ == "__main__":
    main()