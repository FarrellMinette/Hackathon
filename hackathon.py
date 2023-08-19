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
    table_names = [row.table_id for row in result][:-3]
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
    run_sql(f"""Alter table {project_id}.{tables}.drivers ADD COLUMN IF NOT EXISTS overSpeed INT64""")
    for table_name in table_names:
        results = run_sql(f"""UPDATE `{project_id}.{tables}.drivers`
                            SET overSpeed = (
                            SELECT COUNT(*) 
                            FROM `{project_id}.{data}.{table_name}`
                            WHERE speed > road_speed
                            )
                            WHERE vehicleid = {table_name};
                        """)

def insert_gps_lost_count():
    table_names = select_table_names()
    sql = f"""Alter table {project_id}.{tables}.drivers ADD COLUMN IF NOT EXISTS gps_lost_count INT64;"""
    run_sql(sql)
    for table_name in table_names:
        sql = f"""UPDATE `{project_id}.{tables}.drivers`
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
    sql = f"""Alter table {project_id}.{tables}.drivers ADD COLUMN IF NOT EXISTS telematics_off_count INT64;"""
    run_sql(sql)
    for table_name in table_names:
        sql = f"""UPDATE `{project_id}.{tables}.drivers`
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
    sql = f"""Alter table {project_id}.{tables}.drivers ADD COLUMN IF NOT EXISTS normal_braking INT64;"""
    run_sql(sql)
    for table_name in table_names:
        sql = f"""UPDATE `{project_id}.{tables}.drivers`
                            SET normal_braking = (
                            select count(linear_g) from `{project_id}.{data}.{table_name}` WHERE (linear_g < 0.1 AND linear_g > -0.1)
                            )
                            WHERE vehicleid = {table_name};
                        """
        results = run_sql(sql)     

def insert_harsh_braking():
    table_names = select_table_names()
    sql = f"""Alter table {project_id}.{tables}.drivers ADD COLUMN IF NOT EXISTS harsh_braking INT64;"""
    run_sql(sql)
    for table_name in table_names:
        sql = f"""UPDATE `{project_id}.{tables}.drivers`
                            SET harsh_braking = (
                            select count(linear_g) from `{project_id}.{data}.{table_name}` WHERE ((linear_g < 0.3 AND linear_g > -0.3) AND NOT (linear_g < 0.1 AND linear_g > -0.1))
                            )
                            WHERE vehicleid = {table_name};
                        """
        results = run_sql(sql)     

def insert_emergency_braking():
    table_names = select_table_names()
    sql = f"""Alter table {project_id}.{tables}.drivers ADD COLUMN IF NOT EXISTS emergency_braking INT64;"""
    run_sql(sql)
    for table_name in table_names:
        sql = f"""UPDATE `{project_id}.{tables}.drivers`
                            SET emergency_braking = (
                            select count(linear_g) from `{project_id}.{data}.{table_name}` WHERE NOT (linear_g < 0.5 AND linear_g > -0.5)
                            )
                            WHERE vehicleid = {table_name};
                        """
        results = run_sql(sql)                

def insert_claims():
    run_sql(f"""Alter table {project_id}.{tables}.drivers ADD COLUMN IF NOT EXISTS number_of_claims INT64""")
    run_sql(f"""Alter table {project_id}.{tables}.drivers ADD COLUMN IF NOT EXISTS total_claims_cost STRING""")
    sql = f"""UPDATE `{project_id}.{tables}.drivers` AS driver
            SET
                driver.number_of_claims = claims.number_of_claims,
                driver.total_claims_cost = claims.total_claims_cost
            FROM `{project_id}.{data}.claims_data` AS claims
            WHERE driver.vehicleid = claims.vehicleid
            """
    run_sql(sql)

def insert_classified_time_of_day():
    run_sql(f"""Alter table `{project_id}.{tables}.drivers` ADD COLUMN IF NOT EXISTS dangerousTimes INT64""")
    table_names = select_table_names()
    for table_name in table_names:
        results = run_sql(f"""UPDATE `{project_id}.{tables}.drivers`
                            SET dangerousTimes = (
                            SELECT COUNT(*) 
                            FROM `{project_id}.{data}.{table_name}`
                            WHERE EXTRACT(HOUR FROM timestamp) >= 0 
                            AND EXTRACT(HOUR FROM timestamp) < 4
                            )
                            WHERE vehicleid = {table_name};
                        """)
    # Print the query results
        for row in results:
            print("Row:", row)

def insert_average_nmr_stops_per_day():
    run_sql(f"""Alter table `{project_id}.{tables}.drivers_copy` ADD COLUMN IF NOT EXISTS NmrOfStops INT64""")
    table_names = select_table_names()
    for table_name in table_names:
        results = run_sql(f"""UPDATE `{project_id}.{tables}.drivers_copy` AS d
                            SET NmrOfStops = (
                                SELECT COUNT(*) 
                                FROM (
                                    SELECT
                                        timestamp,
                                        ignitionState,
                                        LAG(ignitionState, 1, '') OVER (PARTITION BY vehicleid ORDER BY timestamp) AS prev_ignitionState
                                    FROM
                                        `{project_id}.{data}.{table_name}`
                                ) subquery
                                WHERE d.vehicleid = subquery.vehicleid AND d.timestamp = subquery.timestamp AND d.ignitionState = 'ON' AND subquery.prev_ignitionState <> 'ON'
                            );
                            """)  
    for row in results:
            print("Row:", row)

def insert_classified_area():
    run_sql(f"""Alter table `{project_id}.{tables}.drivers` ADD COLUMN IF NOT EXISTS dangerousAreas INT64""")
    table_names = select_table_names()
    for table_name in table_names:
        results = run_sql(f"""UPDATE `{project_id}.{tables}.drivers`
                            SET dangerousAreas = (
                            SELECT COUNT(*) 
                            FROM `{project_id}.{data}.{table_name}`
                            WHERE MP_NAME IN ('Nyanga', 'Khayelitsha', 'Delft', 'Mitchells Plain', 'Langa', 'Gugulethu', 'Manenberg', 'Bishop Lavis', 'Hanover park')
                            )
                            WHERE vehicleid = {table_name};
                        """)


if __name__ == "__main__":
    # create_driver()
    # insert_vehicleid()
    # alter_driver_overspeed()
    # insert_over_speed_limit()
    # alter_driver_claims_drop()
    # alter_driver_claims()
    # insert_claims()
    # insert_classified_time_of_day()
    insert_average_nmr_stops_per_day()
