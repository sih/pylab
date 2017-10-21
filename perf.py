import pandas as pd
import numpy as np
#
#   Parses a log file and extracts metrics information for each method in a particular transaction.
#   It works out the difference in timings between each method (assuming that the data is encountered sequentially
#   through the file and then find the method that took the longest time
#   avg time
#   max time
#   min time
#   sdev time
#


METHOD_NAME_INDEX = 1
TIMINGS_INDEX=2
TX_ID_INDEX = 4
METRICS_CONTENT_SEPARATOR = "---"
METRICS_SECTION_SEPARATOR = "|"

def process(filename):

    infile = open(filename, "r", encoding="utf-8")
    #

    timingsByTx = {}

    # read the file and extract the metrics details
    for line in infile:
        metricsInfo = line.split(METRICS_CONTENT_SEPARATOR)[1]
        metrics = metricsInfo.split(METRICS_SECTION_SEPARATOR)

        tx = metrics[TX_ID_INDEX].strip().strip("\n")
        methodName = metrics[METHOD_NAME_INDEX].strip()
        time = metrics[TIMINGS_INDEX].strip()

        timingsByTx.setdefault(tx, []).append((methodName, int(time)))

    df = pd.DataFrame.from_dict(timingsByTx, orient='index')

    print(df)
    print("\n")
    print("********************************")
    print("Grab the numeric part of the tuple. These are the method times")

    # get the numeric times part from the dataframe and then apply the difference
    diffs = np.diff(df.applymap(lambda x: x[1]))
    print(diffs)

    print("\n")
    print("********************************")
    print("Calculate the difference between each timing")

    realDiffs = [np.insert(x,0,0) for x in diffs]
    print(realDiffs)

    print("\n")
    print("********************************")
    print("Snip out the method names")
    methodNames = [x for x in df.applymap(lambda x: (x[0])).values]
    print(methodNames)


    print("\n")
    print("********************************")
    print("And append both diffs and method names to the data frame")

    # df['Timings'] = realDiffs
    # df['Methods'] = methodNames

    mts = [dict(zip(x, y)) for x,y in zip(methodNames, realDiffs)]
    df['Method Times'] = mts

    # get the method that took the longest time
    df['Max Method'] = df['Method Times'].apply(lambda x: {max(x, key=x.get): x[max(x, key=x.get)]})


    print(df)

import os
for file in os.listdir("/Users/sid/lab/pylab"):
    if (file.endswith(".log")):
        process(file)
