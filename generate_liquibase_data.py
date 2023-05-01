from dateutil.relativedelta import relativedelta
from datetime import date, datetime, timedelta
from math import log, exp
import argparse
import ndjson
import string
import random
import numpy
import json
import yaml
import time
import os

#----------------------------------------------------------------------------------------------------
# GLOBAL VARIABLES
#----------------------------------------------------------------------------------------------------


########################################################
# NEED TO IMPLEMENT
# • Rollbacks trend down over time
# • Fix multiple entries per changesetID
########################################################

PATH_TO_MDCLOGFILES = "./mdclogfiles/"

# 1 cycle = 1 hour
CYCLES = 1680
deployments_per_cycle = 1
# The DEPLOYMENT_TREND indicates how often we increase the number of docs created per cycle
DEPLOYMENT_TREND = 0.002
# Variance allows the data to have some realistic randomness per cycle
variance = 1

# Used to generate realistic log URLS
URL = "://rdbms.us-west-2.amazonaws.com:"
DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

# All the types of commands that we can generate and how often we see them
ALLCOMMANDS = {"rollbacktargeted":"","rollbackcount":"","rollback":"","updatetotag":"","updatecount":"","update-one":"","update":"","changelogsync":""}
RBT = ["rollbacktargeted"] * 5
RBC = ["rollbackcount"] * 3
RB = ["rollback"] * 1
UDT = ["updatetotag"] * 15
UDC = ["updatecount"] * 3
UDO = ["update-one"] * 1
UD = ["update"] * 45
CLS = ["changelogsync"] * 8
COMMAND_DISTRIBUTION = RBT + RBC + RB + UDT + UDC + UDO + UD + CLS

# How often a command results in a failure (vs. success)
FAILURE_RATE = 0.005

# All the types of databases that we can generate (as well as associated info) and how often we see them
MONGODB = ["mongodb"] * 10
ORACLE = ["oracle"] * 7
MYSQL = ["mysql"] * 5
SNOWFLAKE = ["snowflake"] * 2
DATABASE_DISTRIBUTION = MONGODB + ORACLE + SNOWFLAKE
DATABASE_NUMBERS = {"mongodb": 2, "oracle": 2, "mysql": 2, "mariadb": 2, "postgresql": 2, "db2z": 2, "neo4j": 2, "snowflake": 2}
DATABASE_PORTS = {"mongodb":"27017","oracle":"1521","mysql":"3306","mariadb":"3306","postgresql":"5432","db2z":"25000","neo4j":"7474","snowflake":"443"}
DATABASE_NAMES = ["db1"]

# All the names of apps that we can generate and how often we see them
APP_NAMES_LIST = ["Commercial-CMS","Commercial-OMS","Commercial-Analytics","Commercial-RMI","Commercial-BI","Commercial-Onyx","Commercial-CyclopsBI","Commercial-GambitCard","Commercial-GauntletComm","Commercial-StarkWealth","Commercial-Students","Commercial-Billing","Global-Risk","Commercial-UMS","Global-UWS","Global-WWW","Global-OmniAuth","Global-OAS","Global-MYRP","Global-ASDE","Global-WIBAC","Global-WST","Global-FHCT","Global-MAA","Global-ANG","Global-BillPay","Wealth-DBRDC","Wealth-SMS","Wealth-OFX","Wealth-TNG","Wealth-DBRDC/WIBT","Wealth-OSMP","Consumer-CAES","Consumer-AOESN","Consumer-AOESP","Consumer-MTP","Risk-OPS","Risk-OPSPD","Risk-1BAAS","Risk-GUSER","Management-PSGCD","Management-ISGSM"]
APP_NAMES = []
for app in APP_NAMES_LIST:
  APP_NAMES += [app]*random.randrange(20)

# All the names of authors that we can generate and how often we see them
AA = [["arthur.titian.mcfly", "atm"]] * 3
BB = [["hammurabi.walganus.quijote", "hwq"]] * 19
CC = [["guinevere.idril.poirot", "gip"]] * 4
DD = [["zara.valentinian.finch", "zvf"]] * 5
EE = [["xerxes.olivette.nickleby", "xon"]] * 12
FF = [["aida.lionel.twist", "alt"]] * 9
GG = [["galadriel.merry.skywalker", "gms"]] * 13
HH = [["sauron.justinian.maximus", "sjm"]] * 7
II = [["caspian.malvolio.clovis", "cmc"]] * 6
JJ = [["kratos.hyacintha.scrooge", "khs"]] * 2
KK = [["sherlock.dulcinea.gump", "sdg"]] * 8
AUTHOR_DISTRIBUTION = AA + BB + CC + DD + EE + FF + GG + HH + II + JJ + KK

# All the names of environments that we can generate and how often we see them
DDEV_ENV = ["dev"] * 4
TEST_ENV = ["test"] * 3
STAG_ENV = ["stag"] * 2
PROD_ENV = ["prod"] * 1
ENVIRONMENTS = DDEV_ENV + TEST_ENV + STAG_ENV + PROD_ENV

# All the types of file endings that we can generate and how often we see them
FILETYPES = ['.yml','.sql','.json','.xml']

# Starting number for deployment ID's
deployment_id = 9429453980
changeset_id_tracker = {}

# Used to make sure that app names are consistent across environments, which is important for calculating Lead Time
environment_tracker = {"dev":{"zizurofqcz":"Commercial-CMS"},"test":{"hreruneqnz":"Commercial-OMS"},"stag":{"fpqaulqprt":"Commercial-Analytics"},"prod":{"crnjndgtkz":"Commercial-RMI"}}
environment_incrementer = {"dev":"test","test":"stag","stag":"prod"}
environment_probability = {"dev":0.5,"test":0.75,"stag":0.9,"prod":1}

#----------------------------------------------------------------------------------------------------
# FUNCTION DEFINITIONS
#----------------------------------------------------------------------------------------------------

def get_liquibase_commands (command, filename):
  with open(os.path.join(PATH_TO_MDCLOGFILES, filename), 'r') as f:
    for line in f:
      ALLCOMMANDS[command] += line

def get_date(hour, minute):
  temp_date = (datetime.today() - timedelta(hours=+(CYCLES-hour)))
  return (temp_date - timedelta(minutes=+(minute))).strftime(DATE_FORMAT)

def decision(probability):
  return (random.random() < probability)

def randomword(length):
   letters = string.ascii_lowercase
   return ''.join(random.choice(letters) for i in range(length))

#----------------------------------------------------------------------------------------------------
# READ IN COMMANDS FROM FILE
#----------------------------------------------------------------------------------------------------

for filename in os.listdir(PATH_TO_MDCLOGFILES):
  # with open(os.path.join(PATH_TO_MDCLOGFILES, filename), 'r') as f:
  if "rollbacktargeted" in filename:
    get_liquibase_commands("rollbacktargeted", filename)
  elif "rollbackcount" in filename:
    get_liquibase_commands("rollbackcount", filename)
  elif "rollback" in filename:
    get_liquibase_commands("rollback", filename)
  elif "updatetotag" in filename:
    get_liquibase_commands("updatetotag", filename)
  elif "updatecount" in filename:
    get_liquibase_commands("updatecount", filename)
  elif "update-one" in filename:
    get_liquibase_commands("update-one", filename)
  elif "update" in filename:
    get_liquibase_commands("update", filename)
  elif "changelogsync" in filename:
    get_liquibase_commands("changelogsync", filename)
  elif ".DS_Store" in filename:
    if False:
      print(filename)
  else:
    print("!!!!!!!!!ERROR!!!!!!!!!" + filename)

#----------------------------------------------------------------------------------------------------
# MAIN LOGIC
#----------------------------------------------------------------------------------------------------

# 1 cycle = 1 hour
for cycle in range(1,CYCLES + 1):
  # We want the number of entries data to trend over time: DEPLOYMENT_TREND is a percentage that represents how often we increase the deployments_per_cycle
  if random.random() < DEPLOYMENT_TREND:
    deployments_per_cycle += 1
    # As the number of deployments_per_cycle grows, we shrink the variance otherwise the amount of documents we generate can get kind of crazy
    variance = (1/(deployments_per_cycle+1))+(0.3)

  # Calculate the number of deployments for this cycle based on the variance
  actual_number_of_deployments = deployments_per_cycle + round((random.uniform(-variance, variance)*deployments_per_cycle))

  # generate all the data for each deployment
  for deployment in range(1, actual_number_of_deployments + 1):
    deployment_id += 1
    author = random.choice(AUTHOR_DISTRIBUTION)[1]
    command_choice = random.choice(COMMAND_DISTRIBUTION)
    commands = ndjson.loads(ALLCOMMANDS[command_choice])
    count = 0
    database = random.choice(DATABASE_DISTRIBUTION)

    database_port = DATABASE_PORTS[database]
    database_enpoints = []
    environment = random.choice(ENVIRONMENTS)
    app_name = random.choice(APP_NAMES)

    changelog_filename = ""

    # Ensure that a particular app works it's way across all environments
    if (environment_tracker[environment] and decision(environment_probability[environment])):
      changelog_object = environment_tracker[environment].popitem()
      changelog_filename = changelog_object[0]
      app_name = changelog_object[1]
      if environment in environment_incrementer:
        environment_tracker[environment_incrementer[environment]][changelog_filename] = app_name
    else:
      environment = "dev"
      changelog_filename = randomword(10)
      environment_tracker[environment][changelog_filename] = app_name


    changelog_filepath = "liquibase/changelogs/" + app_name.lower() + "/" + environment + "/" + changelog_filename + random.choice(FILETYPES)

    # For all the databases of a particular type, generate deployments
    for db in range(1, DATABASE_NUMBERS[database] + 1):
      count += 1
      endpoint = "jdcb:" + database + URL + database_port + "/" + app_name + ":" + environment

      # for each command we're working with, generate the appropriate data
      for command in commands:
        
        command["timestamp"] = get_date(cycle,count)
        if 'commandContextFilter' in command:
          command['commandContextFilter'] = environment
        if 'commandLabelFilter' in command:
          command['commandLabelFilter'] = app_name
        if 'changesetId' in command:
          command['changesetId'] = changelog_filename + str(count)
        if 'liquibaseTargetUrl' in command:
            command["liquibaseTargetUrl"] = endpoint
            if "prod" in endpoint:
              FAILURE_RATE = 0.0005
            elif "stag" in endpoint:
              FAILURE_RATE = 0.0025
            elif "test" in endpoint:
              FAILURE_RATE = 0.05
            elif "dev" in endpoint:
              FAILURE_RATE = 0.25
            else:
              FAILURE_RATE = 0.005
        if 'changesetAuthor' in command:
          command["changesetAuthor"] = author
        if 'liquibaseSystemUser' in command:
          command["liquibaseSystemUser"] = "liquibase_admin"
          if decision(0.002):
            command["liquibaseSystemUser"] = random.choice(AUTHOR_DISTRIBUTION)[0]
        if 'liquibaseSystemName' in command:
          command["liquibaseSystemName"] = database + "-db" + str(count) + "-docker-20.10.23"
        if 'deploymentId' in command:
          command["deploymentId"] = deployment_id
        if 'changesetOperationStart' in command:
          command["changesetOperationStart"] = get_date(cycle,count)
        if 'changesetOperationStop' in command:
          if decision(FAILURE_RATE):
            command["changesetOperationStop"] = (datetime.strptime(command["changesetOperationStart"],DATE_FORMAT) + timedelta(minutes=random.randrange(100),seconds=random.randrange(100),microseconds=random.randrange(1000000))).strftime(DATE_FORMAT)
          else:
            command["changesetOperationStop"] = (datetime.strptime(command["changesetOperationStart"],DATE_FORMAT) + timedelta(seconds=random.randrange(100),microseconds=random.randrange(1000000))).strftime(DATE_FORMAT)
        if 'changesetOperationStart' in command:
          startTime = datetime.strptime(command["changesetOperationStart"],DATE_FORMAT)
          if 'changesetOperationStop' in command:
            stopTime = datetime.strptime(command["changesetOperationStop"],DATE_FORMAT)
            diff = stopTime - startTime
            command['changesetOperationDuration'] = int((diff.seconds * 1000) + (diff.microseconds / 1000))
        if 'deploymentOutcome' in command:
          if decision(FAILURE_RATE):
            command["deploymentOutcome"] = "failure"

        # for each command, print out the resulting command in NDJSON format
        print(json.dumps(command))

    












  