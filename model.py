from sklearn.cluster import KMeans
from pandas.plotting import parallel_coordinates
import numpy as np
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import pandas as pd
import os

# Load standardized data from CSV into a pandas DataFrame
df_standardized = pd.read_csv("stand_norm_drivers.csv")

# Perform K-means clustering with k=6
k = 3
kmeans = KMeans(n_clusters=k, random_state=42)
df_standardized["cluster"] = kmeans.fit_predict(df_standardized)

# Set up the figure and create the parallel coordinate plot
plt.figure(figsize=(12, 6))
parallel_coordinates(
    df_standardized,
    "cluster",
    color=("#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b"),
)

# Add legend and adjust layout
plt.legend(title="Cluster")
plt.title(f"Parallel Coordinate Plot of Clusters (k={k})")
plt.xlabel("Features")
plt.ylabel("Standardized Values")
plt.xticks(rotation=45)
plt.tight_layout()

output_directory = "output_plots/"
combined_image_filename = os.path.join(output_directory, f"{k}_parallel_coordinate")

plot_filename = combined_image_filename
plt.savefig(plot_filename)
plt.close()  #
