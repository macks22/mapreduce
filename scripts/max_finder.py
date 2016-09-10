#!/usr/bin/env python
import sys

if __name__ == "__main__":
    index = int(sys.argv[1])

    max_so_far = 0
    for line in sys.stdin:
        fields = line.strip().split(",")
        if fields[index].isdigit():
            val = int(fields[index])
            if (val > max_so_far):
                max_so_far = val

    print(max_so_far)
