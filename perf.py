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


# Turn the raw file in to a DataFrame as per below that we can use to analyze the data.
# The rows are transaction ids and each column represents the timestamp when the method was enttered / existed
#
#     Method A  Method B  Method C  Method D  Method E
# 44        107       201       223       341       NaN
# 223       103       143       203       219       NaN
# 33        106       149       199       207       NaN
# 19        101       104       176       211     351.0
# 51        111       154       204       206       NaN
#
def process(filename):

    infile = open(filename, "r", encoding="utf-8")

    # This dict of dicts ... method times keyed by the transaction id ... is the basis of the DataFrame
    methodTimesByTx = {}

    # read the file and extract the metrics details
    for line in infile:
        metricsInfo = line.split(METRICS_CONTENT_SEPARATOR)[1]  # the half of the string with the metrics info
        metrics = metricsInfo.split(METRICS_SECTION_SEPARATOR)

        tx = metrics[TX_ID_INDEX].strip().strip("\n")           # the transaction id
        methodName = metrics[METHOD_NAME_INDEX].strip()
        time = metrics[TIMINGS_INDEX].strip()

        # not all the methods for each transaction will appear together so collect in a map
        methodTimesByTx.setdefault(tx, {})[methodName] = int(time)

    # This outputs a base table like below
    df = pd.DataFrame(list(methodTimesByTx.values()), index=methodTimesByTx.keys())

    df.fillna(value=0, axis=1, inplace=True)

    print(df)

    return df

#
# Perform some analysis on the resulting dataframe
# For each transaction add:
#   - Elapsed time;
#   - Max method;
#
def analyze(df):

    diffs = list(np.absolute(np.diff(df, axis=1)))

    df['Diffs'] = diffs
    df['Max Method'] = ([df.columns[np.nanargmax(x)] for x in diffs])
    df['Max Method Time'] = [np.nanmax(x) for x in diffs]
    df['Elapsed'] = [np.nansum(x) for x in diffs]

    print(df)

    return df

def summarize(df):
    summaries = {}
    summaries['Average'] = np.mean(df['Elapsed'])
    summaries['Median'] = np.median(df['Elapsed'])
    summaries['Min'] = np.min(df['Elapsed'])
    summaries['Max'] = np.max(df['Elapsed'])
    summaries['SDev'] = np.std(df['Elapsed'])


    return summaries



import os
# for file in os.listdir("/Users/sid/lab/pylab"):
#    if (file.endswith(".log")):
df = process("/Users/sid/lab/pylab/sample-sparse.log")

df = analyze(df)

summaries = summarize(df)
