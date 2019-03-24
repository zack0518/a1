from mpi4py import MPI
import json
import re
from collections import Counter as c
from collections import Counter, defaultdict

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

grids = {}
gridCounter = {}
hashtagCounter = {}

# Parse the gird info from the melbGrid.json
def loadGrid():

    with open("melbGrid.json") as f:
        info = json.load(f)
    # intialize the data structure that used for counting
    for i in info["features"]:
       grid = i["properties"]['id']
       grids[grid] = i["properties"]
       gridCounter[grid] = 0
       hashtagCounter[grid] = {}
       hashtagCounter[grid]["total"] = 0
    # start processing
    twitterProcessor()

# Parse the file line by line, avoid parse the whole file directly
def twitterProcessor():
    lineNum = 0
    assignedLine = rank
    with open("tinyTwitter.json") as f:
        for line in f:
            if assignedLine == lineNum:
                assignedLine = assignedLine + size

                # exclude the first line and the last line of the file
                if not parseCoordinates(line) is None:
                    x , y = parseCoordinates(line)
                    grid = checkWhichGrid(x, y)
                    # count how many twitter in each grid
                    countPerGrid(grid)

                    # parse string that contains the hash tag
                    parsedJSON = parseHashTag(line)
                    # count hash tag in the each grid
                    countHashTagPerGrid(parsedJSON, grid)
            lineNum += 1;
    collectResult()

# collect result from the other processes into master process
def collectResult():
    c1 = c()
    c2 = defaultdict(c)
    sortedKey = []
    gridResult = comm.gather(gridCounter, root=0)
    hashtagResult = comm.gather(hashtagCounter, root=0)
    if rank == 0:
        for r in gridResult:
            c1 = c1 + c(r)
        for item in hashtagResult:
            for k,d in item.items():
                c2[k].update(d)
                c2[k].most_common()
        for k,v in c1.most_common():
            print(k, v)
        print(c2)


def parseCoordinates(line):
    match = re.search(r'\"type\":\"Point\",\"coordinates\":\[(.*?)\]', line)
    if match:
        parsed = json.loads('{' + match.group() + '}')
        return parsed["coordinates"][0], parsed["coordinates"][1]


def countPerGrid(c):
    if c in gridCounter.keys():
        gridCounter[c] += 1


def checkWhichGrid(x, y):
    for c in gridCounter:
        if (x >= grids[c]['xmin'] and x <= grids[c]['xmax']) \
                and (y <= grids[c]['ymax'] and y >= grids[c]['ymin']):
                return c
    return None


# Parse the file line by line, avoid parse the whole file directly
def parseHashTag(line):
    extractedStr = ""
    match = re.search(r'\"hashtags\":\[(.*?)\]',line)
    if match is not None and not closedBracketIndex(match.group()) == match.group():
        matchRest =  re.search(r'\"hashtags\":\[.*',line)
        extractedStr = closedBracketIndex(matchRest.group())

    if not extractedStr == "":
        parsedJSON = json.loads('{'+ extractedStr +'}')
        return parsedJSON

def countHashTagPerGrid(parsedJSON, grid):
    if not parsedJSON is None and not grid is None:
        for i in parsedJSON["hashtags"]:
            tag = i["text"]
            hashtagCounter[grid]["total"] = hashtagCounter[grid]["total"] + 1
            if tag in hashtagCounter[grid]:
                hashtagCounter[grid][tag] += 1
            else:
                hashtagCounter[grid][tag] = 0

# match the closed bracket in a JSON string
def closedBracketIndex(str):
    c = 0
    index = 0
    for i in str:
        index += 1
        if i == "[":
            c += 1
        elif i == "]" and not c == 0:
            c -= 1
            if c == 0:
                break
        if c < 0:
            return 0
    if c == 0:
        return str[0:index]
    else:
        return 0

loadGrid()