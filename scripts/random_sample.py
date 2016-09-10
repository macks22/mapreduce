#!/usr/bin/env python
import sys
import random


if __name__ == "__main__":
    rand_num = int(sys.argv[1])
    for line in sys.stdin:
        if (random.randint(1, 100) <= rand_num):
            print line.strip()
