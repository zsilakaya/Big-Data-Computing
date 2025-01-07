# Big Data Computing - Homework 1

## Project Overview
This repository contains the implementation of Homework 1 for the **Big Data Computing** course. The assignment involves working with Apache Spark to implement and compare exact and approximate algorithms for outlier detection in large datasets. The project focuses on both the accuracy of outlier detection and the computational efficiency of the methods.

## Objective
1. Implement an **Exact Algorithm** for detecting outliers based on pairwise distances.
2. Implement a **MapReduce Approximate Algorithm** using PySpark, leveraging Spark RDDs for distributed computation.
3. Compare the performance of the two algorithms in terms of accuracy and runtime.

## Dataset
The dataset consists of points in a 2D space, represented by their coordinates (x, y). Each point is unique and stored in a CSV file where rows are formatted as:
```
x1,y1
x2,y2
...
```

## Algorithms

### 1. Exact Algorithm
- Computes pairwise distances between all points in the dataset.
- Identifies outliers based on the (M, D)-outlier definition.

### 2. Approximate Algorithm
- Divides the 2D space into grid cells of size determined by parameter D.
- Uses a 3x3 and 7x7 grid around each point to estimate neighborhood density.
- Classifies points as outliers, non-outliers, or uncertain points based on thresholds.

## Implementation Details
### Input
The program accepts the following parameters:
- Path to the dataset file.
- Float `D`: Distance threshold.
- Integers `M`, `K`, `L`: Outlier threshold, number of results to display, and number of partitions.

### Output
The program prints:
1. Total number of points.
2. Results of the Exact Algorithm (if dataset size <= 200,000):
   - Number of outliers.
   - Top K outlier points.
3. Results of the Approximate Algorithm:
   - Number of sure outliers.
   - Number of uncertain points.
   - Top K non-empty grid cells by size.
4. Execution time for both algorithms.


## Technologies Used
- **Programming Language**: Python
- **Framework**: PySpark
