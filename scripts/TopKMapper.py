#!/usr/bin/env python
import sys


def insert_and_shift(values, i, val):
    while i < len(values):
        curval = values[i]
        values[i] = val
        val = curval
        i += 1


def top_value_check(val, max_values):
    for i, max_val in enumerate(max_values):
        if val > max_val:
            insert_and_shift(max_values, i, val)
            break


if __name__ == "__main__":
    K = int(sys.argv[1])
    index = int(sys.argv[2])

    # Largest to smallest
    max_values = [-1] * K

    for line in sys.stdin:
        fields = line.strip().split(",")
        if fields[index].isdigit():
            val = int(fields[index])
            top_value_check(val, max_values)

    print(",".join(map(str, max_values)))
