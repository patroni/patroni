#!/usr/bin/env python

import sys
with open("data/{0}/{0}_cb.log".format(sys.argv[1]), "a+") as log:
    log.write(" ".join(sys.argv[-3:]) + "\n")
