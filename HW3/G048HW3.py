import math
import sys
import random
import threading
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
from collections import defaultdict


# Reservoir Sampling function
def reservoir_sampling(stream, m):
    global t, reservoir_sample
    for xt in stream:
        if t < m:
            reservoir_sample.append(xt)
        else:
            p = random.random()
            if p < m / t:
                evict = random.randint(0, m - 1)
                reservoir_sample[evict] = xt
        t += 1
    return sorted(reservoir_sample)


# Sticky Sampling function
def sticky_sampling(stream, n, epsilon, phi, delta):
    global hash_table
    # Calculate the sampling rate
    r = math.log(1 / (delta * phi)) / epsilon

    # Process each element in the stream
    for x in stream:
        if x in hash_table:
            hash_table[x] += 1
        else:
            p = random.random()
            if p < r / n:
                hash_table[x] = 1

    return hash_table


# Batch processing function
def process_batch(time, batch, m, n, epsilon, phi, delta):
    global stream_length, true_frequent_arr, sticky_sample

    batch_size = batch.count()

    if stream_length[0] >= n:
        return

    stream_length[0] += batch_size
    batch_items = batch.map(int).collect()

    # Update true frequent items count
    for item in batch_items:
        true_frequent_arr[item] += 1

    # Collect reservoir sample
    reservoir_sampling(batch_items, m)

    # Collect sticky sample
    sticky_sampling(batch_items, n, epsilon, phi, delta)

    if stream_length[0] >= n:
        stopping_condition.set()


if __name__ == '__main__':
    assert len(sys.argv) == 6, "USAGE: n phi epsilon delta portExp"

    # Initialize Spark configuration and context
    conf = SparkConf().setMaster("local[*]").setAppName("G048HW3")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 0.01)  # Batch duration of 0.01 seconds
    ssc.sparkContext.setLogLevel("ERROR")

    # Stopping condition
    stopping_condition = threading.Event()

    # Parse command-line arguments
    n = int(sys.argv[1])
    phi = float(sys.argv[2])
    epsilon = float(sys.argv[3])
    delta = float(sys.argv[4])
    portExp = int(sys.argv[5])

    m = math.ceil(1 / phi)

    # Setup global variables
    stream_length = [0]
    true_frequent_arr = defaultdict(int)
    reservoir_sample = []
    sticky_sample = []
    hash_table = {}
    t = 0

    # Setup streaming data source
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    stream.foreachRDD(lambda time, batch: process_batch(time, batch, m, n, epsilon, phi, delta))

    # Start the streaming context
    #print("Starting streaming engine")
    ssc.start()
    #print("Waiting for shutdown condition")
    stopping_condition.wait()
    #print("Stopping the streaming engine")
    ssc.stop(False, True)
    #print("Streaming engine stopped")

    # Compute and print results
    total_items_processed = stream_length[0]

    # True frequent items
    true_frequent_items = [item for item, count in true_frequent_arr.items() if count >= phi * total_items_processed]
    true_frequent_items.sort()

    print("INPUT PROPERTIES")
    print(f"n = {n} phi = {phi} epsilon = {epsilon} delta = {delta} portExp = {portExp}")
    print("EXACT ALGORITHM")
    print(f"Number of items in the data structure = {len(true_frequent_arr)}")
    print(f"Number of true frequent items = {len(true_frequent_items)}")
    print("True frequent items:")
    for item in true_frequent_items:
        print(item)

    reservoir_sample_set = sorted(set(reservoir_sample))
    print("RESERVOIR SAMPLING")
    print(f"Size m of the sample = {m}")
    print(f"Number of estimated frequent items = {len(reservoir_sample_set)}")
    print("Estimated frequent items:")
    for item in reservoir_sample_set:
        if item in true_frequent_items:
            print(item, " +")
        else:
            print(item, " -")

    result = {x: count for x, count in hash_table.items() if count >= (phi - epsilon) * n}
    print("STICKY SAMPLING")
    print(f"Number of items in the Hash Table = {len(hash_table)}")
    print(f"Number of estimated frequent items = {len(result)}")
    print("Estimated frequent items:")
    for item in sorted(result):
        if item in true_frequent_items:
            print(item, " +")
        else:
            print(item, " -")



