from pyspark import SparkContext, SparkConf
from math import sqrt, floor
from time import time
import sys

# Initialize Spark context
conf = SparkConf().setAppName("OutlierDetection")
sc = SparkContext(conf=conf)

# ------
# Code for the exact solution.

def euclidean_distance(point1, point2):
    """Compute the Euclidean distance between two points."""
    x = (point1[0] - point2[0])
    y = (point1[1] - point2[1])
    return sqrt(x * x + y * y)
    # return sqrt((point1[0] - point2[0])**2 + (point1[1] - point2[1])**2)

def ExactOutliers(points, D, M, K):
    """Detect and print the first K (D, M)-outliers from the given points."""
    outliers = []
    outliers_counts = [1]*len(points)
    start_time = time()
    for i in range(len(points)):
        for j in range(i + 1, len(points)):
            if euclidean_distance(points[i], points[j]) <= D:
                outliers_counts[i] += 1
                outliers_counts[j] += 1

    for i in range(len(outliers_counts)):
        if outliers_counts[i] <= M:
            outliers.append((points[i], outliers_counts[i]))

    # Sort the outliers based on their neighborhood size in non-decreasing order
    outliers.sort(key=lambda x: x[1])

    # Print output in the specified format
    print(f"D={D} M={M} K={K} L=2")
    print(f"Number of points = {len(points)}")
    print(f"Number of Outliers = {len(outliers)}")
    for point, _ in outliers[:K]:
        print(f"Point: {point}")
    print(f"Running time of ExactOutliers = {int((time() - start_time) * 1000)} ms")
    return outliers

# ------
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

def MRApproxOutliers(pointsRDD, D, M, K):
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

    topKCells = cellsRDD.takeOrdered(K, key=lambda x: -x[1])

    print("Number of sure outliers =", sureOutliers)
    print("Number of uncertain points =", uncertainPoints)
    for count, cell_id in topKCells:
        print(f"Cell: {count}  Size = {cell_id}")
    print(f"Running time of MRApproxOutliers = {int((time() - start_time) * 1000)} ms")

# Parse command-line arguments
filePath = sys.argv[1] # File path to the input data
D = float(sys.argv[2])    # Distance threshold
M = int(sys.argv[3])    # Minimum number of neighbors
K = int(sys.argv[4])    # Number of cells to display
L = int(sys.argv[5])    # Number of partitions

# Print command-line arguments
print(f"File path: {filePath}")
print(f"Distance threshold (D): {D}")
print(f"Minimum number of neighbors (M): {M}")
print(f"Number of cells to display (K): {K}")
print(f"Number of partitions (L): {L}")

# Reading File
rawData = sc.textFile(filePath)
inputPoints = rawData.map(lambda line: line.split(",")).map(lambda x: (float(x[0]), float(x[1]))).repartition(L)

# Total number of points
total_points = inputPoints.count()
print(f"Total number of points: {total_points}")

# Run ExactOutliers if the dataset is small
listOfPoints = inputPoints.collect()
if len(listOfPoints) <= 200000:
    ExactOutliers(listOfPoints, D, M, K)

# Always run MRApproxOutliers
MRApproxOutliers(inputPoints, D, M, K)

