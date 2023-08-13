import pandas as pd
import os
from google.cloud import bigquery

def look_at_data(filename):
    df = pd.read_csv(filename)
    print(df.head(10))


def summary(df, describe=True):
    if describe:
        print(df.describe())

    missing = df.isnull().sum()
    count = df.count()

    # Create DataFrames for "missing" and "count" with appropriate column names
    missing_df = pd.DataFrame({'column': missing.index, 'missing': missing.values})
    count_df = pd.DataFrame({'column': count.index, 'count': count.values})

    # Merge the two DataFrames based on the 'column' column
    result = pd.merge(missing_df, count_df, on='column')
    print(result)

def create_big_boi():
    claims_df = pd.read_csv("claims_data.csv")
    
    # Define the folder containing CSV files
    folder_path = "./telematics_data/final/"

    # Loop through each CSV file and read it into a DataFrame
    dfs = []  # List to store DataFrames

    for file in os.listdir(folder_path):
        if file.endswith(".csv"):
            csv_file = os.path.join(folder_path, file)
            df = pd.read_csv(csv_file)
            dfs.append(df)
    
    vehicle_ids = []
    for df in dfs:
        vehicle_ids.append(df["vehicleid"][0])
    filtered_dfs = []

    # Iterate through each vehicle ID
    for id in vehicle_ids:
        # Filter the claims_df for the current vehicle ID
        filtered_df = claims_df[claims_df['vehicleid'] == id]
        # Append the filtered DataFrame to the list
        filtered_dfs.append(filtered_df)

    # Concatenate the filtered DataFrames to create the final "claims" DataFrame
    claims = pd.concat(filtered_dfs)
    claims = claims.fillna(0)

    bigboi = pd.DataFrame(vehicle_ids, columns=["vehicleid"])
    bigboi = pd.merge(bigboi, claims, on="vehicleid", how="outer")

    bigboi = pd.DataFrame(vehicle_ids, columns=["vehicleid"])
    bigboi = pd.merge(bigboi, claims, on="vehicleid", how="outer")
    
    bigboi['bad driver'] = 0
    for index, row in bigboi.iterrows():
        if row['number of claims'] != 0:
            bigboi.at[index, 'bad driver'] = 1

    # Initialize a dictionary to store average distance per stop for each vehicle ID
    avg_distance_per_stop = {}
    avg_stops_per_day = {}

    bigboi['average distance per stop'] = 0
    bigboi['average number of stops per day'] = 0
    for df in dfs:
        stops_df = distance_per_stop_df(df)
        avg_dist = 0
        stops_count = 0
        count = 0
        avg_stops = 0
        for index, row in stops_df.iterrows():
            stops_count += row['number of stops']
            count += 1
            avg_dist += row['average distance per stop'] if row['average distance per stop'] != 0 else 0
            avg_stops += row['number of stops'] if row['number of stops'] != 0 else 0
        if stops_count != 0:
            avg_dist /= stops_count
        if count != 0:
            avg_stops /= count
        # Store the calculated average distance per stop in the dictionary
        avg_distance_per_stop[df['vehicleid'][0]] = avg_dist
        avg_stops_per_day[df['vehicleid'][0]] = avg_stops

    # Use the stored dictionary to update the 'average distance per stop' column in bigboi
    bigboi['average distance per stop'] = bigboi['vehicleid'].map(avg_distance_per_stop)
    # Update the 'average number of stops per day' column in the bigboi DataFrame
    bigboi['average number of stops per day'] = bigboi['vehicleid'].map(avg_stops_per_day)

    return bigboi


def distance_per_stop_df(driver_df):
    # Initialize a dictionary to store the stop counts and distance traveled for each partition date
    stop_counts = {}
    distance_traveled = {}
    driver_df['odometer'] = driver_df['odometer'].fillna(0)

    # Iterate through the rows in the DataFrame
    for index, row in driver_df.iterrows():
        date = row['partition_date']
        ignition_state = row['ignitionState']
        odometer = row['odometer']

        # Check if the partition date is already in the dictionaries, if not, add it
        if date not in stop_counts:
            stop_counts[date] = 0
            distance_traveled[date] = 0

        # Increment the stop count if ignitionState is "OFF"
        if ignition_state == "OFF":
            stop_counts[date] += 1

            # Find the next row with ignition state "ON" for the same date
            next_row_index = index + 1
            while (next_row_index < len(driver_df) and
                   driver_df.loc[next_row_index, 'partition_date'] == date and
                   driver_df.loc[next_row_index, 'ignitionState'] == "ON"):
                next_row_index += 1

            # Calculate distance only if a subsequent trip is found
            if next_row_index < len(driver_df):
                next_row = driver_df.iloc[next_row_index]
                distance_traveled[date] += next_row['odometer'] - odometer

    # Convert the dictionaries to DataFrames
    stops_data = [{'date': date, 'number of stops': count} for date, count in stop_counts.items()]
    distances_data = [{'date': date, 'average distance per stop': dist / count if count > 0 else 0} for date, (count, dist) in zip(stop_counts.keys(), zip(stop_counts.values(), distance_traveled.values()))]

    stops_df = pd.DataFrame(stops_data)
    distances_df = pd.DataFrame(distances_data)

    # Merge the two DataFrames based on the 'date' column
    result_df = pd.merge(stops_df, distances_df, on='date')
        
    return result_df

def update_table_schema(table_id, schema_updates):
    # Initialize the BigQuery client
    client = bigquery.Client()

    # Get the existing table
    table_ref = client.dataset('your_dataset_id').table(table_id)
    table = client.get_table(table_ref)

    # Update the schema to include missing columns
    new_schema = table.schema
    for column_name, column_type in schema_updates.items():
        if column_name not in [field.name for field in new_schema]:
            new_schema.append(bigquery.SchemaField(column_name, column_type))

    # Update the table with the new schema
    try:
    # Update the table with the new schema
        table.schema += schema_updates
        client.update_table(table, ["schema"])
        print("Table schema update succeeded.")
    except Exception as e:
        print("Table schema update failed:", str(e))

def update_cloud(df, update_columns):
    table_id = "hackathon-1123-395609.hackathon_tables"
    # Dictionary of columns you want to add to the table and their data types
    schema_updates = {
        "bad driver": "INTEGER",
        "average distance per stop": "DOUBLE",
        "average number of stops per day": "DOUBLE",
    }

    # Update the table schema
    update_table_schema(table_id, schema_updates)
    # Convert DataFrame to a list of dictionaries
    update_data = df.to_dict(orient='records')

    # Initialize the BigQuery client
    client = bigquery.Client()

    # Specify the target table and columns to update
    table_id = 'your_project_id."hackathon-1123-395609.hackathon_tables"'

    # Perform the update
    query = (
        f"UPDATE `{table_id}` "
        f"SET value = t.value "
        f"FROM UNNEST(@update_data) AS t "
        f"WHERE {table_id}.id = t.id"
    )

    # Create a query job
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("update_data", "STRUCT<id INT64, value FLOAT64>", update_data)]
    )

    query_job = client.query(query, job_config=job_config)

    # Wait for the job to complete
    query_job.result()

def some_things():
    df = pd.read_csv('average_stops_and_distance_per_stop.csv')
    new_df = pd.DataFrame(columns=df.columns)

    # Iterate through the rows in the original DataFrame 'df'
    for index, row in df.iterrows():
        if row['bad_driver'] == 1:
            # Copy the entire row to the new DataFrame 'new_df'
            new_df = new_df.append(row)
            new_df.loc[index,'bad_driver'] = True
            df.loc[index,'bad_driver'] = True
        else:
            df.loc[index,'bad_driver'] = False
    print(df.head(20))
    print(new_df.head(20))
    df.to_csv("bigboi.csv", index=False)

def return_missing(df):
    missing_df  =  pd.DataFrame(columns=df.columns)
    for index, row in df.iterrows():
        if pd.isna(row['average_distance_per_stop']):
            missing_df = missing_df.append(row, ignore_index=True)
    return missing_df

def update_missing_cols(df, missing_df):
    new_df = df.copy()
    # Iterate through the vehicleids in missing_df
    for id in missing_df['vehicleid']:
        # Load the data from the CSV file
        vehicle_df = pd.read_csv(f'./telematics_data/final/{id}.csv')
        # Calculate the distance per stop 
        stops_df = distance_per_stop_df(vehicle_df)

        # Find the index where the 'vehicleid' matches in the new_df and update the 'average_distance_per_stop'
        index_to_update = new_df.index[new_df['vehicleid'] == id]
        if not index_to_update.empty:
            new_df.loc[index_to_update, 'average_distance_per_stop'] = stops_df['average distance per stop'].iloc[0]

    return new_df
        

if __name__== "__main__":
    df = pd.read_csv('./our_data/drivers.csv')
    missing_df = return_missing(df)
    new_df = update_missing_cols(df, missing_df)
    new_df.to_csv("new_stops.csv", index=False)