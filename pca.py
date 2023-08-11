import os
import re
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import numpy as np

# Path to the directory containing CSV files
folder_path = "telematics_data/final"

# Select a specific driver file
driver_file = "207010096.csv"  # Replace with the actual filename

# Read the driver data
driver_data = pd.read_csv(os.path.join(folder_path, driver_file))
print(driver_data.shape)
print(type(driver_data)) 

driver_data = driver_data.replace(r',+',',', regex=True)
print(driver_data.iloc[1])

columns_to_check = ['id', 'vehicleid', 'terminal_id', 'gps_accuracy', 'DC_NAME', 'MN_NAME', 'PR_NAME', 'country_name', 'timestamp', 'odometer', 'recieved_ts','created_ts']

# Iterate through specific columns and process if cardinality is one
for column in columns_to_check:
    card = driver_data[column].nunique()
    if ( card == 1 or card > (9/10)*driver_data.shape[0] ) :
        stored_value = driver_data[column].iloc[0]
        driver_data.drop(columns=[column], inplace=True)
        print(f"Column '{column}' had cardinality '{card}'. Stored value: {stored_value}")

cardinality = driver_data.nunique()
print("Cardinality of 1\n",cardinality[cardinality==5])
print("Cardinality above 9/10 the sample size\n",cardinality[cardinality>(driver_data.shape[0]*9)/10])

driver_data = driver_data.drop_duplicates()
driver_data = driver_data.dropna()

# Extract features (excluding non-numeric columns)
features = driver_data.select_dtypes(include=['number'])

# Standardize the features
scaler = StandardScaler()
standardized_features = scaler.fit_transform(features)

# Perform PCA
pca = PCA()
principal_components = pca.fit_transform(standardized_features)

# Explained variance ratio
explained_variance_ratio = pca.explained_variance_ratio_
explained_variance_ratio = explained_variance_ratio / np.sum(explained_variance_ratio)

# # Plot explained variance ratio
# plt.figure(figsize=(8, 6))
# plt.bar(range(1, len(explained_variance_ratio) + 1), explained_variance_ratio)
# plt.xlabel('Principal Components')
# plt.ylabel('Explained Variance Ratio')
# plt.title('Explained Variance Ratio per Principal Component')
# plt.show()

count=0
# Print the explained variance ratios
for i, ratio in enumerate(explained_variance_ratio, start=1):
    print(f"Principal Component {i}: Explained Variance Ratio = {ratio:.4f}")
    if(ratio/explained_variance_ratio[0] < 0.33): 
        break
    else:
        count = count+1

print(count)

# Decide on the number of components to keep based on the plot and explained variance
num_components_to_keep = count  # Update this based on your analysis

# Transform the data using the selected number of components
selected_principal_components = principal_components[:, :num_components_to_keep]

# Create a DataFrame with the selected principal components
selected_components_df = pd.DataFrame(selected_principal_components, columns=[f'PC{i}' for i in range(1, num_components_to_keep + 1)])

# Combine the selected components with the original non-numeric columns
final_data = pd.concat([driver_data.drop(columns=features.columns), selected_components_df], axis=1)

final_data = final_data.drop_duplicates()
print(final_data.shape)