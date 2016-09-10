#!/usr/bin/env python
"""
We take a simple approach here, assuming that all mapper results will fit into memory
in a single reducer.

The input K must match the input for the mapper.

"""
import sys


if __name__ == "__main__":
    K = int(sys.argv[1])
    all_nums = []

    for line in sys.stdin:
        nums = map(int, line.split(','))
        all_nums += nums

    nums.sort(reverse=True)

    topk = map(str, nums[:K])
    for num_str in topk:
        print(num_str)
