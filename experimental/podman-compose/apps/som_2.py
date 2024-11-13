import numpy as np
import random
import setuptools.dist
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from sklearn.metrics.pairwise import euclidean_distances

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("SOM-Anomaly-Detection") \
    .getOrCreate()

class SelfOrganizingMap:
    def __init__(self, grid_size=(10, 10), num_iterations=100, learning_rate=0.1, radius=None):
        # Grid size: (height, width)
        self.grid_size = grid_size
        self.num_iterations = num_iterations
        self.learning_rate = learning_rate
        self.radius = radius or max(grid_size) // 2
        self.weights = None

    def initialize_weights(self, data):
        # Randomly initialize weights within the data range
        num_features = data.shape[1]
        self.weights = np.random.rand(self.grid_size[0], self.grid_size[1], num_features)

    def train(self, data):
        for i in range(self.num_iterations):
            # Pick a random data point from the dataset
            idx = random.randint(0, len(data) - 1)
            sample = data[idx]

            # Find the best matching unit (BMU)
            bmu_idx = self.find_bmu(sample)

            # Update the weights of the SOM grid
            self.update_weights(sample, bmu_idx, self.learning_rate)

    def find_bmu(self, sample):
        # Calculate the Euclidean distance between the sample and all the nodes
        distances = np.linalg.norm(self.weights - sample, axis=-1)
        bmu_idx = np.unravel_index(np.argmin(distances), distances.shape)
        return bmu_idx

    def update_weights(self, sample, bmu_idx, learning_rate):
        for i in range(self.grid_size[0]):
            for j in range(self.grid_size[1]):
                distance_to_bmu = np.linalg.norm(np.array([i, j]) - np.array(bmu_idx))
                if distance_to_bmu < self.radius:
                    influence = np.exp(-(distance_to_bmu ** 2) / (2 * (self.radius ** 2)))
                    self.weights[i, j] += influence * learning_rate * (sample - self.weights[i, j])

    def predict(self, data):
        distances = euclidean_distances(data, self.weights.reshape(-1, self.weights.shape[-1]))
        bmu_idx = np.argmin(distances, axis=1)
        return bmu_idx


def anomaly_score(data, som_model, threshold=0.5):
    bmu_idx = som_model.predict(data)
    # Calculate the Euclidean distance between the data point and its BMU
    distances = np.linalg.norm(data - som_model.weights.reshape(-1, som_model.weights.shape[-1])[bmu_idx], axis=1)
    
    # Anomaly is determined based on distance threshold
    scores = distances > threshold
    return scores


# Example Usage
# Load Datae
df = spark.read.csv("/opt/spark-apps/your_data.csv", header=True, inferSchema=True)

# Preprocess data: Convert all features into a single vector column
features_col = [col for col in df.columns if col != 'label']  # Assuming 'label' is the target column
assembler = VectorAssembler(inputCols=features_col, outputCol="features")
df = assembler.transform(df)

# Convert DataFrame to RDD for distributed processing
rdd_data = df.select("features").rdd.map(lambda row: np.array(row['features']))

# Sample data as a NumPy array (convert to 2D array)
data = np.array(rdd_data.collect())

# Initialize and train SOM
som = SelfOrganizingMap(grid_size=(10, 10), num_iterations=1000, learning_rate=0.1)
som.initialize_weights(data)
som.train(data)

# Anomaly Detection
scores = anomaly_score(data, som, threshold=0.5)

# Identify anomalous points
anomalies = np.where(scores)[0]

# Show anomalies
print("Anomalous indices:", anomalies)

# Stop Spark session
spark.stop()

