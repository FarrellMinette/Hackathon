from google.cloud import bigquery

## Setup
# Set your Google Cloud project ID
project_id = "hackathon-1123-395609"
data = "hackhathon_data"
tables = "hackathon_tables"

# Initialize BigQuery client
client = bigquery.Client(project=project_id)

# Run sql query
def run_sql(sql):
    script = sql
    query_job = client.query(sql)
    result = query_job.result()
    return result

## Create the 'drivers' table
def create_driver():
    sql_create_driver_table = f"""
    CREATE OR REPLACE TABLE `{project_id}.{tables}.drivers` (
        vehicleid INT64
    )
    """
    query_job = client.query(sql_create_driver_table)
    query_job.result()


## Select all the vehicle ids and store in table_names
def select_table_names():
    result = run_sql(f"""SELECT table_id FROM `{project_id}.{data}.__TABLES__` """)
    table_names = [row.table_id for row in result][:-2]
    return table_names

## Add vehicle ids to the drivers table in hackathon_tables 
# Loop through table names and extract vehicleid using SQL
def insert_vehicleid():
    table_names = select_table_names()
    for table_name in table_names:
        results = run_sql(f"""
        SELECT distinct(vehicleid) FROM `{project_id}.{data}.{table_name}`
        """)

        # Insert vehicleid values into the drivers table
        for row in results:
            vehicleid = row.vehicleid
            run_sql(f"""INSERT INTO {project_id}.{tables}.drivers (vehicleid) VALUES ({vehicleid})""")

def alter_driver_overspeed():
    run_sql(f"""Alter table {project_id}.{tables}.drivers ADD COLUMN overSpeed INT64""")

def alter_driver_claims():
    run_sql(f"""Alter table {project_id}.{tables}.drivers ADD COLUMN number_of_claims INT64""")
    run_sql(f"""Alter table {project_id}.{tables}.drivers ADD COLUMN total_claims_cost STRING""")

def alter_driver_claims_drop():
    run_sql(f"""Alter table {project_id}.{tables}.drivers DROP COLUMN claims""")
    # run_sql(f"""Alter table {project_id}.{tables}.drivers DROP COLUMN total_claims_cost""")


def insert_over_speed_limit():
    table_names = select_table_names()
    for table_name in table_names:
        results = run_sql(f"""UPDATE `{project_id}.{tables}.drivers`
                            SET overSpeed = (
                            SELECT COUNT(*) 
                            FROM `{project_id}.{data}.{table_name}`
                            WHERE speed > road_speed
                            )
                            WHERE vehicleid = {table_name};
                        """)

def insert_claims():
    sql = f"""UPDATE `{project_id}.{tables}.drivers` AS driver
            SET
                driver.number_of_claims = claims.number_of_claims,
                driver.total_claims_cost = claims.total_claims_cost
            FROM `{project_id}.{data}.claims_data` AS claims
            WHERE driver.vehicleid = claims.vehicleid
            """
    run_sql(sql)

# create_driver()
# insert_vehicleid()
# alter_driver_overspeed()
# insert_over_speed_limit()
# alter_driver_claims_drop()
# alter_driver_claims()
# insert_claims()