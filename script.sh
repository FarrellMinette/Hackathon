# Replace this with the actual path to your CSV files on Google Cloud Storage
CSV_PATH="gs://data-bucket-hackathon/"

# Loop through CSV filenames and load into corresponding tables
for csv_file in $(gsutil ls $CSV_PATH*.csv)
do
    # Extract the table name from the CSV filename (without .csv extension)
    table_name=$(basename "$csv_file" .csv)

    # Construct the full table name (dataset_name.table_name)
    full_table_name=hackhathon_data.$table_name

    # Load the CSV file into the table using the schema from schema.json
    bq load --source_format=CSV --skip_leading_rows=1 --schema=schema.json $full_table_name "$csv_file"
done