#!/usr/bin/env python
import sys


if __name__ == "__main__":
    last_key, running_sum, count = (None, 0.0, 0)

    for line in sys.stdin:
        key, val = line.split('\t')

        if last_key and last_key != key:
            print("%s\t%s" % (last_key, str(running_sum / count)))
            running_sum, count = (0.0, 0)

        last_key = key
        running_sum += float(val)
        count += 1

    print("%s\t%s" % (last_key, str(running_sum / count)))
