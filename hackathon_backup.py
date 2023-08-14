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

## Create the 'driver_backup' table
def create_driver():
    sql_create_driver_table = f"""
    CREATE OR REPLACE TABLE `{project_id}.{tables}.driver_backup` (
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

## Add vehicle ids to the driver_backup table in hackathon_tables 
# Loop through table names and extract vehicleid using SQL
def insert_vehicleid():
    table_names = select_table_names()
    for table_name in table_names:
        results = run_sql(f"""
        SELECT distinct(vehicleid) FROM `{project_id}.{data}.{table_name}`
        """)

        # Insert vehicleid values into the driver_backup table
        for row in results:
            vehicleid = row.vehicleid
            run_sql(f"""INSERT INTO {project_id}.{tables}.driver_backup (vehicleid) VALUES ({vehicleid})""")
    

def alter_driver_claims_drop():
    run_sql(f"""Alter table {project_id}.{tables}.driver_backup DROP COLUMN claims""")

def insert_over_speed_limit():
    table_names = select_table_names()
    run_sql(f"""Alter table {project_id}.{tables}.driver_backup ADD COLUMN IF NOT EXISTS overSpeed INT64""")
    for table_name in table_names:
        results = run_sql(f"""UPDATE `{project_id}.{tables}.driver_backup`
                            SET overSpeed = (
                            SELECT COUNT(*) 
                            FROM `{project_id}.{data}.{table_name}`
                            WHERE speed > 1.1*road_speed
                            )
                            WHERE vehicleid = {table_name};
                        """)

def insert_gps_lost_count():
    table_names = select_table_names()
    sql = f"""Alter table {project_id}.{tables}.driver_backup ADD COLUMN IF NOT EXISTS gps_lost_count INT64;"""
    run_sql(sql)
    for table_name in table_names:
        sql = f"""UPDATE `{project_id}.{tables}.driver_backup`
                            SET gps_lost_count = (
                            SELECT COUNT(*) 
                            FROM `{project_id}.{data}.{table_name}`
                            WHERE event_description='GPS_Lost'
                            )
                            WHERE vehicleid = {table_name};
                        """
        results = run_sql(sql)  

def insert_telematics_off_count():
    table_names = select_table_names()
    sql = f"""Alter table {project_id}.{tables}.driver_backup ADD COLUMN IF NOT EXISTS telematics_off_count INT64;"""
    run_sql(sql)
    for table_name in table_names:
        sql = f"""UPDATE `{project_id}.{tables}.driver_backup`
                            SET telematics_off_count = (
                            SELECT COUNT(*) 
                            FROM `{project_id}.{data}.{table_name}`
                            WHERE event_description='Power OFF'
                            )
                            WHERE vehicleid = {table_name};
                        """
        results = run_sql(sql)       

def insert_normal_braking():
    table_names = select_table_names()
    sql = f"""Alter table {project_id}.{tables}.driver_backup ADD COLUMN IF NOT EXISTS normal_braking INT64;"""
    run_sql(sql)
    for table_name in table_names:
        sql = f"""UPDATE `{project_id}.{tables}.driver_backup`
                            SET normal_braking = (
                            select count(linear_g) from `{project_id}.{data}.{table_name}` WHERE (linear_g < 0.1 AND linear_g > -0.1)
                            )
                            WHERE vehicleid = {table_name};
                        """
        results = run_sql(sql)     

def insert_harsh_braking():
    table_names = select_table_names()
    sql = f"""Alter table {project_id}.{tables}.driver_backup ADD COLUMN IF NOT EXISTS harsh_braking INT64;"""
    run_sql(sql)
    for table_name in table_names:
        sql = f"""UPDATE `{project_id}.{tables}.driver_backup`
                            SET harsh_braking = (
                            select count(linear_g) from `{project_id}.{data}.{table_name}` WHERE ((linear_g < 0.3 AND linear_g > -0.3) AND NOT (linear_g < 0.1 AND linear_g > -0.1))
                            )
                            WHERE vehicleid = {table_name};
                        """
        results = run_sql(sql)     

def insert_emergency_braking():
    table_names = select_table_names()
    sql = f"""Alter table {project_id}.{tables}.driver_backup ADD COLUMN IF NOT EXISTS emergency_braking INT64;"""
    run_sql(sql)
    for table_name in table_names:
        sql = f"""UPDATE `{project_id}.{tables}.driver_backup`
                            SET emergency_braking = (
                            select count(linear_g) from `{project_id}.{data}.{table_name}` WHERE ((linear_g < 0.5 AND linear_g > -0.5) AND NOT (linear_g < 0.3 AND linear_g > -0.3))
                            )
                            WHERE vehicleid = {table_name};
                        """
        results = run_sql(sql)      

def insert_critical_braking():
    table_names = select_table_names()
    sql = f"""Alter table {project_id}.{tables}.drivers ADD COLUMN IF NOT EXISTS critical_braking INT64;"""
    run_sql(sql)
    for table_name in table_names:
        sql = f"""UPDATE `{project_id}.{tables}.drivers`
                            SET critical_braking = (
                            select count(linear_g) from `{project_id}.{data}.{table_name}` WHERE NOT (linear_g < 0.5 AND linear_g > -0.5)
                            )
                            WHERE vehicleid = {table_name};
                        """
        results = run_sql(sql)        

def insert_corner_speeding():
    table_names = select_table_names()
    for table_name in table_names:
        results = run_sql(f"""Alter table {project_id}.{tables}.drivers ADD COLUMN IF NOT EXISTS cornerSpeeding INT64;
                            UPDATE `{project_id}.{tables}.drivers`
                            SET cornerSpeeding = (
                            SELECT COUNT(*) 
                            FROM `{project_id}.{data}.{table_name}`
                            WHERE speed > road_speed
                            AND event_description ='Corner'
                            )
                            WHERE vehicleid = {table_name};
                        """)      

def insert_claims():
    run_sql(f"""Alter table {project_id}.{tables}.driver_backup ADD COLUMN IF NOT EXISTS number_of_claims INT64""")
    run_sql(f"""Alter table {project_id}.{tables}.driver_backup ADD COLUMN IF NOT EXISTS total_claims_cost FLOAT64""")
    sql = f"""UPDATE `{project_id}.{tables}.driver_backup` AS driver
            SET
                driver.number_of_claims = claims.number_of_claims,
                driver.total_claims_cost = claims.total_claims_cost
            FROM `{project_id}.{data}.claims_data` AS claims
            WHERE driver.vehicleid = claims.vehicleid
            """
    run_sql(sql)

def insert_idle_time():
    table_names = select_table_names()
    sql = f"""ALTER TABLE {project_id}.{tables}.driver_backup ADD COLUMN IF NOT EXISTS idle_ratio FLOAT64;"""
    run_sql(sql)
    for table_name in table_names:
        sql = f"""
                UPDATE `{project_id}.{tables}.driver_backup`
                SET idle_ratio = (
                    SELECT idle / total as idle_ratio
                    FROM (
                        SELECT 
                            SUM(CASE WHEN event_description LIKE 'Idle%' THEN 1 ELSE 0 END) AS idle,
                            COUNT(*) AS total
                        FROM `{project_id}.{data}.{table_name}`
                    ) subquery
                )
                WHERE vehicleid = {table_name};
                """
        results = run_sql(sql)  

def test():
    sql = f"""Select * from {project_id}.{tables}.driver_backup"""
    results = run_sql(sql)
    for result in results:
        print(result)

def insert_claims():
    sql = f"""ALTER TABLE `{project_id}.{tables}.driver_backup`
            ADD COLUMN IF NOT EXISTS number_of_claims INT64,
            ADD COLUMN IF NOT EXISTS total_claims_cost FLOAT64;

            UPDATE `{project_id}.{tables}.driver_backup` AS driver
            SET driver.number_of_claims = claims.number_of_claims,
                driver.total_claims_cost = CAST(claims.total_claims_cost AS FLOAT64)
            FROM `hackhathon_data.claims_data_cleaned` AS claims
            WHERE driver.vehicleid = claims.vehicleid;
            """
    run_sql(sql)

# update `hackathon_tables.driver_backup` as driver
# set driver.bad_driver = updated.bad_driver,
# driver.average_distance_per_stop = updated.average_distance_per_stop,
# driver. average_number_of_stops_per_day = updated.average_number_of_stops_per_day
# from `hackathon-1123-395609.hackathon_tables.updated_stops` as updated
# where driver.vehicleid=updated.vehicleid

# create_driver()
# insert_vehicleid()
insert_over_speed_limit()
insert_corner_speeding()
# insert_gps_lost_count()
# insert_telematics_off_count()
# insert_normal_braking()
# insert_harsh_braking()
# insert_emergency_braking()
# insert_critical_braking()
# insert_idle_time()