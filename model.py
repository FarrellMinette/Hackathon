import numpy as np
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import pandas as pd

# Load normalized data from CSV into a pandas DataFrame
df_normalized = pd.read_csv("normalized_drivers.csv")

# Drop the 'VehicleID' column if it exists (assuming it's the first column)
if "VehicleID" in df_normalized.columns:
    df_normalized = df_normalized.drop(columns=["VehicleID"])

# Create an array of k values to test
k_values = range(1, 11)  # Test k from 1 to 10

# Initialize an empty list to store the within-cluster sum of squares (inertia) for each k
inertia_values = []

# Perform K-means clustering for each value of k
for k in k_values:
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    kmeans.fit(df_normalized)
    inertia_values.append(kmeans.inertia_)

# Plot the within-cluster sum of squares (inertia) against different values of k
plt.plot(k_values, inertia_values, marker="o")
plt.title("Elbow Method for Optimal k")
plt.xlabel("Number of Clusters (k)")
plt.ylabel("Within-Cluster Sum of Squares (Inertia)")
plt.xticks(k_values)
plt.tight_layout()

# Save the plot as an image file
plot_filename = "kmeans_elbow_plot.png"
plt.savefig(plot_filename)
plt.close()  # Close the figure to free up memory
