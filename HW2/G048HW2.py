from pyspark import SparkContext, SparkConf
from scipy.spatial.distance import cdist
from math import floor, sqrt
from time import time
import sys
import numpy as np

# Initialize Spark context
conf = SparkConf().setAppName("OutlierDetection2")
conf.set("spark.locality.wait","0s")
sc = SparkContext(conf=conf)

def min_euclidean_distance(point, centers):
    """Compute the minimum Euclidean distance from a point to a list of centers."""

    return np.min(cdist([point], centers, metric='euclidean'))


def SequentialFFT(P, K):
    """Code for sequential Farthest First Traversal."""

    P = np.array(P)  # Convert list to NumPy array
    centers = []

    # Initialize first center
    centers.append(0)
    min_distances = np.linalg.norm(P - P[centers[0]], axis=1)

    # Selecting the remaining K-1 centers
    for _ in range(1, K):
        farthest_point_index = np.argmax(min_distances)
        centers.append(farthest_point_index)

        distances = np.linalg.norm(P - P[farthest_point_index], axis=1)
        min_distances = np.minimum(min_distances, distances)

    return P[centers]

def MRFFT(inputPointsRDD, K):
    """Code for MR Farthest First Traversal."""

    # Round 1: Local centers
    start_time_round1 = time()
    localCentersRDD = inputPointsRDD.mapPartitions(
        lambda it: [SequentialFFT(list(it), K)]
    ).flatMap(lambda x: x)
    localCenters = localCentersRDD.collect()
    end_time_round1 = time()

    # Round 2: Global centers from the coresets
    start_time_round2 = time()
    globalCenters = SequentialFFT(localCenters, K)
    globalCentersBroadcast = sc.broadcast(globalCenters)
    end_time_round2 = time()

    # Round 3: Calculate the maximum radius
    start_time_round3 = time()
    radius = inputPointsRDD.map(
        lambda p: min_euclidean_distance(p, globalCentersBroadcast.value)
    ).reduce(max)
    end_time_round3 = time()

    # Print running times for each round
    print(f"Running time of MRFFT Round 1 = {int((end_time_round1 - start_time_round1) * 1000)} ms")
    print(f"Running time of MRFFT Round 2 = {int((end_time_round2 - start_time_round2) * 1000)} ms")
    print(f"Running time of MRFFT Round 3 = {int((end_time_round3 - start_time_round3) * 1000)} ms")

    return radius

# ----
# Code for the approximate solution.
def get_cell_identifier(x, y, Lambda):
    """Computes the cell identifier (i, j) for a point."""
    i = floor(x / Lambda)
    j = floor(y / Lambda)
    return (i, j)

def expand_cell(cell):
    """Generates the 3x3 and 7x7 grids of cell identifiers around a given cell."""
    i, j = cell
    R3 = [(i + di, j + dj) for di in range(-1, 2) for dj in range(-1, 2)]
    R7 = [(i + di, j + dj) for di in range(-3, 4) for dj in range(-3, 4)]
    return R3, R7

def compute_N3_N7(cell, cellInfo):
    """Calculate the cell identifiers for the 3x3 and 7x7 grids and sum up the counts for each grid."""
    R3, R7 = expand_cell(cell)
    N3 = sum(cellInfo.get((i, j), 0) for i, j in R3)
    N7 = sum(cellInfo.get((i, j), 0) for i, j in R7)
    return N3, N7

def classify_point(point, cellInfo, Lambda, M):
    """Classify each point based on its neighborhood's size within the 3x3 and 7x7 grids."""
    cell = get_cell_identifier(point[0], point[1], Lambda)
    N3, N7 = compute_N3_N7(cell, cellInfo)
    if N3 > M:
        return ('non-outlier', 1)
    elif N7 <= M:
        return ('sure-outlier', 1)
    else:
        return ('uncertain', 1)

def MRApproxOutliers(pointsRDD, D, M):
    start_time = time()
    Lambda = D / (sqrt(2) * 2)

    cellsRDD = pointsRDD.map(lambda point: (get_cell_identifier(point[0], point[1], Lambda), 1)).reduceByKey(lambda a, b: a + b)

    cellInfo = cellsRDD.collectAsMap()
    cellInfoBroadcast = sc.broadcast(cellInfo)

    # Classify each point by directly computing its neighborhood's size within the 3x3 and 7x7 grids
    classifiedPointsRDD = pointsRDD.map(lambda point: classify_point(point, cellInfoBroadcast.value, Lambda, M))

    # Aggregate classified results
    results = classifiedPointsRDD.countByKey()

    sureOutliers = results.get('sure-outlier', 0)
    uncertainPoints = results.get('uncertain', 0)

    print("Number of sure outliers =", sureOutliers)
    print("Number of uncertain points =", uncertainPoints)
    print(f"Running time of MRApproxOutliers = {int((time() - start_time) * 1000)} ms")

# Parse command-line arguments
filePath = sys.argv[1]
M = int(sys.argv[2])
K = int(sys.argv[3])
L = int(sys.argv[4])

print(f"{filePath} M={M} K={K} L={L}")

# Reading File
rawData = sc.textFile(filePath)
inputPoints = rawData.map(lambda line: line.split(",")).map(lambda x: (float(x[0]), float(x[1]))).repartition(L)
print("Number of points =", inputPoints.count())

radius = MRFFT(inputPoints, K)
print("Radius =", radius)
MRApproxOutliers(inputPoints, radius, M)



