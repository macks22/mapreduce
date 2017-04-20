#!/usr/bin/env python
"""
Output values for use with Hadoop Streaming value histogram.
Extract two columns from a csv file.

"""
import sys


if __name__ == "__main__":
    index1 = int(sys.argv[1])
    index2 = int(sys.argv[2])

    for line in sys.stdin:
        fields = line.split(',')
        print("ValueHistogram:%s\t%s" % (fields[index1], fields[index2]))
