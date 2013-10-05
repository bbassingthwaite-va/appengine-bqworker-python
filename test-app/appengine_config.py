import os
import sys

PATHS = [
    ['lib'],
]

def setSysPath():
    """
    Add lib as primary libraries directory
    """
    c = os.path.abspath(os.path.dirname(__file__))
    for item in PATHS:
        p = os.path.join(c, *item)
        if not p in sys.path:
            sys.path[1:1] = [p]

setSysPath()

bqworker_DEFAULT_PAGE_SIZE = 10
bqworker_API_PROJECT_ID = 'fantasm-hr'
