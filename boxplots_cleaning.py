import pandas as pd
import matplotlib.pyplot as plt
import os
import seaborn as sns
from sklearn.preprocessing import MinMaxScaler


# Load data from CSV into a pandas DataFrame
df = pd.read_csv("drivers.csv")

# Drop the 'DriverID' column if it exists (assuming it's the first column)
if "VehicleID" in df.columns:
    df = df.drop(columns=["VehicleID"])
# Set Seaborn style
sns.set(style="whitegrid")

# Create the output directory if it doesn't exist
output_directory = "output_plots/unnormalised"
os.makedirs(output_directory, exist_ok=True)

# Create a combined boxplot for all variables and save it as an image
plt.figure(figsize=(12, 8))  # Adjust figure size if needed
sns.boxplot(data=df)

# Set a title and labels
plt.title("Boxplots for Driver Features")
plt.xlabel("Features")
plt.ylabel("Values")

# Rotate x-axis labels if needed
plt.xticks(rotation=45)

# Save the plot as an image file in the output directory
combined_image_filename = os.path.join(output_directory, "combined_boxplot.png")
plt.tight_layout()
plt.savefig(combined_image_filename)
plt.close()  # Close the figure to free up memory

# Create boxplots using Seaborn and save them as images
for column in df.columns:
    plt.figure(figsize=(10, 6))  # Adjust figure size if needed
    sns.boxplot(data=df[column])

    # Set a title and labels
    plt.title(f"Boxplot for {column}")
    plt.xlabel(column)
    plt.ylabel("Values")

    # Rotate x-axis labels if needed
    plt.xticks(rotation=0)

    # Save the plot as an image file in the output directory
    image_filename = os.path.join(output_directory, f"{column}_boxplot.png")
    plt.savefig(image_filename)
    plt.close()  # Close the figure to free up memory


# Normalize the data using Min-Max scaling
scaler = MinMaxScaler()
df_normalized = pd.DataFrame(scaler.fit_transform(df), columns=df.columns)

output_directory = "output_plots/normalised"
os.makedirs(output_directory, exist_ok=True)

# Create normalized boxplots for each variable and save them as images
for column in df_normalized.columns:
    plt.figure(figsize=(10, 6))  # Adjust figure size if needed
    sns.boxplot(data=df_normalized[column])

    # Set a title and labels
    plt.title(f"Normalized Boxplot for {column}")
    plt.xlabel(column)
    plt.ylabel("Normalized Values")

    # Rotate x-axis labels if needed
    plt.xticks(rotation=45)

    # Save the normalized plot as an image file in the output directory
    normalized_image_filename = os.path.join(
        output_directory, f"{column}_normalized_boxplot.png"
    )
    plt.tight_layout()
    plt.savefig(normalized_image_filename)
    plt.close()  # Close the figure to free up memory

# Create a combined boxplot for all variables and save it as an image
plt.figure(figsize=(12, 8))  # Adjust figure size if needed
sns.boxplot(data=df_normalized)

# Set a title and labels
plt.title("Boxplots for Driver Features")
plt.xlabel("Features")
plt.ylabel("Values")

# Rotate x-axis labels if needed
plt.xticks(rotation=0)

# Save the plot as an image file in the output directory
combined_image_filename = os.path.join(
    output_directory, "combined_boxplot_normalised.png"
)
plt.tight_layout()
plt.savefig(combined_image_filename)
plt.close()  # Close the figure to free up memory
