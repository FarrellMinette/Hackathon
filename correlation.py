import pandas as pd
from google.cloud import bigquery
import matplotlib.pyplot as plt
import seaborn as sns

# Define your project ID, dataset ID, and table name
project_id = "hackathon-1123-395609"
dataset_id = "hackathon_tables"
table_name = "driver_backup"

# Authenticate with your Google Cloud credentials
client = bigquery.Client(project=project_id)

# Construct the BigQuery table reference
table_ref = f"{project_id}.{dataset_id}.{table_name}"

# Query to get all numeric columns from the table
query = f"""
    SELECT *
    FROM {table_ref}
"""

# Read data from BigQuery into a Pandas DataFrame
df = client.query(query).to_dataframe()

# Calculate the correlation matrix
correlation_matrix = df.corr()

# Get the indices of the top correlated features
corr_unstacked = correlation_matrix.unstack()
sorted_corr_indices = corr_unstacked.abs().sort_values(ascending=False).index

# Extract the top 20 unique pairs of features with their correlation values
top_corr_feature_pairs = []
seen_features = set()

for index in sorted_corr_indices:
    feature1, feature2 = index
    if feature1 != feature2 and len(top_corr_feature_pairs) < 20:
        if (feature1, feature2) not in seen_features and (feature2, feature1) not in seen_features:
            correlation = correlation_matrix.loc[feature1, feature2]
            top_corr_feature_pairs.append((feature1, feature2, correlation))
            seen_features.add((feature1, feature2))

print("Top 20 Unique Pairs of Features with Their Correlation Values:")
for pair in top_corr_feature_pairs:
    print(pair)

# Get the highest positive and negative correlations
highest_positive_corr = correlation_matrix.unstack().sort_values(ascending=False).head(1).iloc[0]
highest_negative_corr = correlation_matrix.unstack().sort_values().head(1).iloc[0]

print("\nHighest Positive Correlation:")
print(highest_positive_corr)

print("\nHighest Negative Correlation:")
print(highest_negative_corr)

plt.figure(figsize=(10, 8))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm')
plt.title("Correlation Matrix")
plt.savefig("corr.png")
