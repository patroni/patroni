#!/usr/bin/env python
from patroni import main

import os

if __name__ == '__main__':

    if os.getenv("PATRONI_DEBUG_MODE"):
        # XXX Visual Code specific https://github.com/microsoft/ptvsd/issues/1443
        # create processes by spawning new Python interpreters instead of forking the current one 
        import multiprocessing
        multiprocessing.set_start_method('spawn', True)

    main()
