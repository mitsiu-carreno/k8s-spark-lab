"""

from pyspark.sql import SparkSession
import numpy as np

# Initialize Spark session
spark = SparkSession.builder.appName("DistributedSOM").getOrCreate()

# Example: Load the data into a DataFrame
data = spark.read.csv("/opt/spark-apps/your_data.csv", header=True, inferSchema=True)

# Convert the data into an RDD for distributed computation
rdd = data.rdd.map(lambda row: np.array([row['feature1'], row['feature2'], row['feature3'], row['feature4']]))

# Initialize parameters for SOM
m = 10  # Number of rows in SOM grid
n = 10  # Number of columns in SOM grid
dim = 4  # Number of features (input dimensionality)
learning_rate = 0.5
sigma = 1.0
num_iterations = 100

# Initialize the SOM weights randomly (m x n grid with 'dim' dimensions)
weights = np.random.rand(m, n, dim)

# Function to find the Best Matching Unit (BMU) for a given sample
def find_bmu(sample, weights):
    # Compute Euclidean distances between the sample and all neurons in the SOM grid
    distances = np.linalg.norm(weights - sample, axis=2)
    # Find the index of the BMU (the neuron with the smallest distance to the sample)
    bmu_idx = np.unravel_index(np.argmin(distances), (m, n))
    return bmu_idx

# Function to update the SOM weights based on the BMU and learning parameters
def update_weights(sample, bmu_idx, weights, learning_rate, sigma):
    bmu_row, bmu_col = bmu_idx
    for i in range(m):
        for j in range(n):
            # Compute the distance between the current neuron and the BMU
            dist = np.linalg.norm(np.array([bmu_row, bmu_col]) - np.array([i, j]))
            # Apply the Gaussian neighborhood function (theta)
            theta = np.exp(-dist**2 / (2 * sigma**2))
            # Update the weights for the neuron
            weights[i, j] += learning_rate * theta * (sample - weights[i, j])
    return weights

# Function to train the SOM on the given data
def train_som(rdd, weights, learning_rate, sigma, num_iterations):
    for t in range(num_iterations):
        # Optionally sample a random mini-batch from the RDD (10% of data in each iteration)
        batch = rdd.sample(False, 0.1).collect()  # Random mini-batch (10% of data for each iteration)
        
        for sample in batch:
            # Find the Best Matching Unit (BMU) for the sample
            bmu_idx = find_bmu(sample, weights)
            # Update the weights of the SOM
            weights = update_weights(sample, bmu_idx, weights, learning_rate, sigma)
    
    return weights

# Train the SOM using the RDD
trained_weights = train_som(rdd, weights, learning_rate, sigma, num_iterations)

# The trained weights now hold the final SOM grid after training
print("Trained SOM weights:")
print(trained_weights)



spark.stop()

"""

from pyspark.sql import SparkSession

# Start a Spark session
spark = SparkSession.builder \
    .appName("SOM Example") \
    .getOrCreate()

data = spark.read.csv("/opt/spark-apps/your_data.csv", header=True, inferSchema=True)


data.show(5)

# Convert to Pandas DataFrame for processing with SOM
pandas_data = data.toPandas()

# Optionally, select relevant columns for training
features = pandas_data[['feature1', 'feature2', 'feature3']].values  # Adjust based on your columns


import numpy as np

class SOM:
    def __init__(self, m, n, dim, learning_rate=0.5, sigma=1.0, num_iterations=100):
        """
        Initialize the SOM grid and parameters.
        m, n: Grid dimensions (rows, columns)
        dim: The dimensionality of the input data
        learning_rate: The initial learning rate
        sigma: The initial radius of the neighborhood
        num_iterations: Number of iterations to train the SOM
        """
        self.m = m
        self.n = n
        self.dim = dim
        self.learning_rate = learning_rate
        self.sigma = sigma
        self.num_iterations = num_iterations
        
        # Initialize the SOM grid with random weights
        self.weights = np.random.rand(m, n, dim)
        
    def train(self, data):
        """
        Train the SOM using the input data.
        """
        for t in range(self.num_iterations):
            # Decay learning rate and sigma over time
            lr = self.learning_rate * (1 - t / self.num_iterations)
            sigma = self.sigma * (1 - t / self.num_iterations)
            
            for sample in data:
                # Find the Best Matching Unit (BMU)
                bmu_idx = self.find_bmu(sample)
                self.update_weights(sample, bmu_idx, lr, sigma)
                
    def find_bmu(self, sample):
        """
        Find the Best Matching Unit (BMU) for a given input sample.
        """
        bmu_idx = np.argmin(np.linalg.norm(self.weights - sample, axis=2))
        return np.unravel_index(bmu_idx, (self.m, self.n))
    
    def update_weights(self, sample, bmu_idx, lr, sigma):
        """
        Update the weights of the SOM based on the BMU and learning parameters.
        """
        bmu_row, bmu_col = bmu_idx
        for i in range(self.m):
            for j in range(self.n):
                # Calculate the Euclidean distance between current neuron and BMU
                dist = np.linalg.norm(np.array([bmu_row, bmu_col]) - np.array([i, j]))
                
                # Apply Gaussian neighborhood function
                theta = np.exp(-dist**2 / (2 * sigma**2))
                
                # Update the weights with a decay based on distance and learning rate
                self.weights[i, j] += lr * theta * (sample - self.weights[i, j])

    def get_bmu_distances(self, data):
        """
        Calculate the distance from each data point to its BMU in the SOM grid.
        """
        distances = []
        for sample in data:
            bmu_idx = self.find_bmu(sample)
            bmu_row, bmu_col = bmu_idx
            dist = np.linalg.norm(self.weights[bmu_row, bmu_col] - sample)
            distances.append(dist)
        return np.array(distances)


# Define the SOM parameters
som = SOM(m=10, n=10, dim=features.shape[1], learning_rate=0.5, sigma=1.0, num_iterations=100)

# Train the SOM on the data
som.train(features)


# Calculate the distance of each sample to its BMU
distances = som.get_bmu_distances(features)

# Define a threshold for anomaly detection (e.g., based on percentile)
threshold = np.percentile(distances, 95)  # 95th percentile as an example threshold

# Identify anomalies
anomalies = distances > threshold

# Print or display the anomalies
anomalous_data = pandas_data[anomalies]
print("Detected Anomalies:")
print(anomalous_data)

