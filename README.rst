Overview: This project contains the implemenation of:

- IB-Tree: IBTree.py

- IB+-Tree: IBPlusTree.py

- Tuning algorithm (on-going)

- Evaluation, measurement tools (on-going)

- Dataset: listbuckets.txt & listbuckets_2.txt (extracted from New York City Taxi and Uber data sets)

Detailed descriptions:

- Evaluation_1.py: to verify the correctness of IB-Tree & IB+-Tree implemenation

- Evaluation_2.py: to evaluate the IB-Tree performance (in terms of number of scanned buckets and number of return buckets)

- Evaluation_3.py: to evaluate the performance of IB+-Tree

- IBTree.py: the class of IB-Tree

- IBPlusTree.py: the class of IB+-Tree & IB+DataBuffer

- Prepare_Data.py: to prepare input data for the evaluation (read data from postgresQL)

- Datafiles: listbuckets.txt, listbuckets_2.txt, listbuckets_sorted.txt, listbuckets_sorted_2.txt

- Prepare_Dataset.py: to download and insert data into postgresQL

- PLITypes package: some useful classes

- tools package: some useful tools/operations

- ExperimentalResults directory: contains experiment result (*.txt)
    + Experiment of IB-Tree: IBTree_Test*.txt
    + Experiment of IB+-Tree: IBPlusTree_Test*.txt
