from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
import numpy as np
from minisom import MiniSom
import pandas as pd
#import matplotlib.pyplot as plt

# Step 1: Initialize Spark session
spark = SparkSession.builder.appName("MiniSomAnomalyDetection").getOrCreate()

# Step 2: Load and preprocess data (example with random data for illustration)
# Here, we assume your data has three features: feature1, feature2, and feature3

data = [
    (0, 1.0, 2.0, 3.0),  # normal data
    (1, 2.0, 3.0, 4.0),  # normal data
    (2, 3.0, 4.0, 5.0),  # normal data
    (3, 4.0, 5.0, 6.0),  # normal data
    (4, 50.0, 60.0, 70.0),  # anomaly
    (5, 100.0, 200.0, 300.0),  # anomaly
]

columns = ['id', 'feature1', 'feature2', 'feature3']
df = spark.createDataFrame(data, columns)

# Step 3: Assemble features into a single vector column (required for ML algorithms)
vector_assembler = VectorAssembler(inputCols=['feature1', 'feature2', 'feature3'], outputCol='features')
df = vector_assembler.transform(df)

# Step 4: Collect the data to the driver for use with MiniSom
data_for_som = df.select("features").rdd.map(lambda row: row['features'].toArray()).collect()

# Step 5: Convert the collected data into a NumPy array for MiniSom
data_for_som = np.array(data_for_som)

# Step 6: Apply MiniSom
# Initialize MiniSom: 3x3 grid, 3 features, 100 iterations, learning rate of 0.5, and sigma of 1.0
som = MiniSom(x=3, y=3, input_len=3, sigma=1.0, learning_rate=0.5)

# Step 7: Train the SOM with the data
som.train(data_for_som, num_iteration=100)

# Step 8: Identify Anomalies Based on Distance to BMU
# You can calculate the distance between each input and its BMU
anomalies = []

for i, data_point in enumerate(data_for_som):
    # Find the BMU for the data point
    bmu = som.winner(data_point)
    # Calculate the distance to the BMU (Euclidean distance between the data point and the BMU's weight vector)
    bmu_distance = np.linalg.norm(som.get_weights()[bmu[0], bmu[1]] - data_point)
    
    print(i)
    print(bmu_distance)
    print(data_point)
    print("-"*100)
    
    # Set a threshold for anomaly detection (this threshold is chosen empirically)
    threshold = 3.0  # You may need to adjust this based on your data
    if bmu_distance > threshold:
        anomalies.append((i, bmu, bmu_distance))

# Step 9: Display the anomalies
print(f"Anomalies detected (index, BMU, distance to BMU):")
for anomaly in anomalies:
    print(anomaly)

# Optional: Visualize the SOM map and anomalies
"""
plt.figure(figsize=(10, 8))
som.pcolor()  # Display the SOM map
for anomaly in anomalies:
    plt.plot(data_for_som[anomaly[0]][0], data_for_som[anomaly[0]][1], 'ro')  # Red points for anomalies
plt.title('SOM with Anomalies')
plt.show()
"""
