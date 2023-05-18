# Data Generator for Liquibase

This is a rough but effective script for generating data for Liquibase logs.

Essentially, the `generate_liquibase_data.py` loads all the files from `mdclogfiles` and then uses them as templates to generate lots of Liquibase logs.


## Important variables/parameters

### CYCLES
TLDR: How much data do you want to generate? This is how many times the main `for` loop will iterate.

Slightly more nuanced, one `cycle` = one `hour`. So, from the perspective of time series data, `CYCLES = 720` --> 720 hours / 24 hours = 30 days.

## How To

### Generate Data

To generate data, set the number of `CYCLES` you want to generate, and then run the following command:

`python3 ./generate_liquibase_data.py`

### Upload Data to Elastic Cloud

1. Navigate to http://cloud.elastic.co
2. Create a cloud instance
3. Take the cloud credentials and add them to the `load_data_into_elasticsearch.py` file
4. Generate Liquibase data using the `generate_liquibase_data.py` script above
5. Point the `FILE_PATH` in the `load_data_into_elasticsearch.py` file to the data you just generated
6. Run `python3 ./load_data_into_elasticsearch.py`

## MDCLOGFILES

Source: https://github.com/mariochampion/mdc-log-tests