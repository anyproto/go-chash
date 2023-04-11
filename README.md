# CHash

A lightweight library for load distribution.

![Alpha](https://img.shields.io/badge/version-alpha-green.svg)

## Description

Combines modular hashing and consistent hashing. The algorithm distributes partitions between nodes using consistent hashing, then uses modular hashing to determine partition for key (and thus the node for key).

The algorithm limits the load on a node, which makes distribution more or less even as you can see in the table below. 

| Nodes | Next | Median Partitions | Min Partitions | Max Partitions | Partitions Moved |
|-------|------|-------------------|----------------|----------------|------------------|
| 16    | 17   | 753               | 704            | 754            | 6.49%            |
| 17    | 18   | 708               | 674            | 709            | 6.42%            |
| 18    | 19   | 669               | 625            | 670            | 5.69%            |
| 19    | 20   | 634               | 595            | 635            | 5.53%            |
| 20    | 21   | 602               | 556            | 603            | 5.53%            |
| ...   | ...  | ...               | ...            | ...            | ...              |
| 66    | 67   | 182               | 164            | 183            | 2.22%            |
| 67    | 68   | 180               | 159            | 181            | 2.30%            |
| 68    | 69   | 177               | 159            | 178            | 2.38%            |
| 69    | 70   | 174               | 161            | 175            | 2.37%            |
| 70    | 71   | 172               | 154            | 173            | 2.05%            |
