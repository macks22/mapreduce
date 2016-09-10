#!/usr/bin/env python
import sys


if __name__ == "__main__":
    for line in sys.stdin:
        fields = line.split(',')
        num_claims = fields[8]
        if (num_claims and num_claims.isdigit()):
            country_name = fields[4][1:-1]  # take off quotes on edges
            print("%s\t%s" % (country_name, num_claims))
