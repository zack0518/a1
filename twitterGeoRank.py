from mpi4py import MPI
import numpy as np
import json
import re
from collections import Counter as c

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

grids = {}
counter = {}

# Parse the gird info from the melbGrid.json
def loadGrid():
    with open("melbGrid.json") as f:
        info = json.load(f)
    for i in info["features"]:
       grid = i["properties"]['id']
       grids[grid] = i["properties"]
       counter[grid] = 0
    count()

# Parse the file line by line, avoid parse the whole file directly
def count():
    lineNum = 0
    assignedLine = rank
    with open("tinyTwitter.json") as f:
        for line in f:
            if assignedLine == lineNum:
                assignedLine = assignedLine + size
                match = re.search(r'\"type\":\"Point\",\"coordinates\":\[(.*?)\]',line)
                if match:
                    parsed = json.loads('{' + match.group() + '}')
                    countGrid(parsed["coordinates"][0], parsed["coordinates"][1])
            lineNum += 1;
    collectResult()

# collect result from the other processes into master process
def collectResult():
    collection = c()
    result = comm.gather(counter, root=0)
    if rank == 0:
        for r in result:
            collection = collection + c(r)
        print(collection)


# check which grid the given cooridates belong to
def countGrid(x, y):
        for c in counter:
            if (x >= grids[c]['xmin'] and x <= grids[c]['xmax']) \
                    and (y and y >= grids[c]['ymin']):
                counter[c] += 1;
                return counter


loadGrid()
#count()
#countGrid()