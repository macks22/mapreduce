#!/usr/bin/env python
import sys


if __name__ == "__main__":
    index = int(sys.argv[1])

    for line in sys.stdin:
        fields = line.split(',')
        print("LongValueSum:%s\t1" % fields[index])
