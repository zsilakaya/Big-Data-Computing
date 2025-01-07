# Big Data Computing - Homework 3

## Project Overview
This repository contains the implementation of Homework 3 for the **Big Data Computing** course. The task involves using the Spark Streaming API to process a real-time stream of integer items and compare the effectiveness of two methods for identifying frequent items:

1. **Reservoir Sampling**
2. **Sticky Sampling**

The program connects to a stream emitted by a server and processes the items in batches to compute true frequent items and estimate frequent items using the aforementioned methods.

## Objective
1. Process a stream of \( n \) items emitted from the server at a specified port.
2. Compute the following:
   - True frequent items with respect to a frequency threshold \( \phi \).
   - An \( m \)-sample of the stream using Reservoir Sampling (\( m = \lceil 1/\phi \rceil \)).
   - \( \epsilon \)-Approximate Frequent Items using Sticky Sampling with a confidence parameter \( \delta \).
3. Compare the outputs of the two methods against the true frequent items.

## Implementation Details
### Input Parameters
The program accepts the following command-line arguments:
1. \( n \): Number of items to process.
2. \( \phi \): Frequency threshold (float in (0, 1)).
3. \( \epsilon \): Accuracy parameter (float in (0, 1)).
4. \( \delta \): Confidence parameter (float in (0, 1)).
5. `portExp`: Port number (integer).

### Output
The program prints:
1. Input parameters.
2. **Exact Algorithm**:
   - Size of the data structure.
   - Number of true frequent items.
   - True frequent items (in increasing order, one per line).
3. **Reservoir Sampling**:
   - Size \( m \) of the Reservoir sample.
   - Number of estimated frequent items.
   - Estimated frequent items (in increasing order, one per line) with a `+` or `-` indicating whether they are true frequent items.
4. **Sticky Sampling**:
   - Size of the Hash Table.
   - Number of estimated frequent items.
   - Estimated frequent items (in increasing order, one per line) with a `+` or `-` indicating whether they are true frequent items.

## Technologies Used
- **Programming Language**: Python
- **Framework**: PySpark (Streaming API)
