from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
import numpy as np
from minisom import MiniSom

# Step 1: Initialize Spark session
spark = SparkSession.builder.appName("MiniSomExample").getOrCreate()

# Sample data (for illustration; replace with your actual data)
data = [(0, 1.0, 2.0, 3.0), (1, 2.0, 3.0, 4.0), (2, 3.0, 4.0, 5.0),
        (3, 4.0, 5.0, 6.0), (4, 5.0, 6.0, 7.0)]

# Step 2: Create PySpark DataFrame
columns = ['id', 'feature1', 'feature2', 'feature3']
df = spark.createDataFrame(data, columns)

# Step 3: Assemble features into a single vector column (required for ML algorithms)
vector_assembler = VectorAssembler(inputCols=['feature1', 'feature2', 'feature3'], outputCol='features')
df = vector_assembler.transform(df)

# Step 4: Collect the data to driver (MiniSom doesn't work with distributed data)
data_for_som = df.select("features").rdd.map(lambda row: row['features'].toArray()).collect()

# Step 5: Convert the collected data into a NumPy array for MiniSom
data_for_som = np.array(data_for_som)

# Step 6: Apply MiniSom
# Initialize MiniSom - 3x3 grid, 3 features per sample, 100 iterations, learning rate of 0.5, and sigma of 1.0
som = MiniSom(x=3, y=3, input_len=3, sigma=1.0, learning_rate=0.5)

# Step 7: Train the SOM with the data
som.train(data_for_som, num_iteration=100)

# Step 8: Visualize the SOM or analyze it
# You can use pcolor() to visualize the SOM grid
#som.pcolor()  # Display the SOM map

# Alternatively, you can use som.get_weights() to examine the weight vectors of the neurons
weights = som.get_weights()
print(weights)

# Step 9: Find the Best Matching Unit (BMU) for a new data point
sample_point = np.array([3.0, 4.0, 5.0])  # Example data point
bmu = som.winner(sample_point)
print(f"The BMU for the sample point is located at {bmu}")

