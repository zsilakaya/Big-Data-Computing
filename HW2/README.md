# Big Data Computing - Homework 2

## Project Overview
This repository contains the implementation of Homework 2 for the **Big Data Computing** course. The assignment builds upon Homework 1 by modifying the approximate outlier detection algorithm to use the radius of a k-center clustering as the distance parameter (𝐷). The project evaluates the scalability and effectiveness of the modified algorithm when executed on large datasets in a distributed environment using Apache Spark on the CloudVeneto cluster.

## Objective
1. Modify the **MRApproxOutliers** function from Homework 1 to remove the parameter \( K \) and adjust functionality.
2. Implement a **SequentialFFT** function to compute k-center clustering centers using the Farthest-First Traversal algorithm.
3. Develop the **MRFFT** method to compute k-center clustering using a MapReduce approach with the following steps:
   - Compute a coreset of points.
   - Determine centers using the coreset and the SequentialFFT algorithm.
   - Compute the clustering radius using Spark's distributed computation.
4. Integrate the modified **MRApproxOutliers** with the output radius (𝐷) from **MRFFT** to detect outliers.

## Implementation Details
### Functions
#### SequentialFFT
- **Input**: A set of points (list) and an integer \( K \).
- **Output**: A list of \( K \) cluster centers.
- **Complexity**: \( O(|P|.K) \).

#### MRFFT
- **Input**: An RDD of points and an integer \( K \).
- **Output**: Radius \( R \) of the clustering.
- **Steps**:
  1. Compute a coreset using MapReduce (Round 1).
  2. Use SequentialFFT to compute centers from the coreset (Round 2).
  3. Broadcast centers and compute the radius \( R \) using a distributed reduce operation (Round 3).

#### Modified MRApproxOutliers
- **Input**: RDD of points, radius \( D \), and parameters \( M \) and \( K \).
- **Output**: Number of outliers and execution time.
- **Modification**: Removed the \( K \) parameter for cell size sorting and adjusted outputs accordingly.

### Program Workflow
1. Accepts command-line arguments:
   - Path to input dataset.
   - Integers \( M, K, L \) (outlier threshold, clustering centers, and partitions).
2. Reads input dataset into an RDD of points, partitioned into \( L \) partitions.
3. Computes total number of points.
4. Executes **MRFFT** to determine radius \( R \).
5. Uses \( R \) as \( D \) and runs the modified **MRApproxOutliers** algorithm.
6. Prints running times and results.

## Technologies Used
- **Programming Language**: Python
- **Framework**: PySpark
