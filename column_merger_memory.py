# -*- coding: utf-8 -*-
"""
Created on Fri Dec 30. 2016

@author: gublermr

This script merges the different collums from a raw data export.
The following requirements have to be met:
- All input files have to be placed in one folder.
- The files have to be labeld with part1, part2....
- The files can be split into serveral export chunks with identical length
    srf-player-ios_20161226-20161226_part1
    srf-player-ios_20161226-20161226_part2
    srf-player-ios_20161227-20161227_part1
    srf-player-ios_20161227-20161227_part2

Paramters:
folder_in: the folder, where all input files are placed. The files from the same timerange have to be labeld identically, except the suffix "part"
folder_out: folder, wehere the outputfile is being written.

The files are being merged on the information of browser and ns_utc of the visit.
The first file (part1) needs to have the borwser information in column 1 and the ns_utc information in column 4
All additional files need the browser information in column 1 and the ns_utc information in column 3
If a visit has no line in one of the parts 2 onward, the values are  being subsituted with "0"
"""
import os
import pandas as pd
import time


# A single line from part 1 is being matched to the lines of part 2 and more.
# if in a file no corresponding line is being found, a the values are being retruned as "0"
from networkx.algorithms.shortest_paths.weighted import all_pairs_dijkstra_path

# Zeitmessung einschalten
# abschätzen, wie lange das Skrip laufen würde bei ganzer Datenmenge
start_time = time.time()

folder_in = "SRG Play App/sample spet-nov-50/orig"
folder_out= "SRG Play App/sample spet-nov-50/cm"

time_slice = []
part_slice = []

all_parts=[]

for name in os.listdir(folder_in):
    pos = name.find("part")
    if (name[pos:] not in part_slice):
        part_slice.append(name[pos:])
    if name[:pos] not in time_slice:
        time_slice.append(name[:pos])


# iterate through all time slices of the export
for slice in time_slice:
    # read all parts of the timeslice into dataframes
    for part in part_slice:
        all_parts.append(pd.read_csv(folder_in+"/"+slice+part, header=None, sep='\t', lineterminator='\n'))
        #all_parts.append(np.loadtxt(folder_in+"/"+slice+part, delimiter='\t')

    # set browser id and ns_utc as indexes for first part
    first_df = all_parts[0]
    result = first_df.set_index([0,3])

    # loop though all other parts
    for i in range(1, len(all_parts)):
        df = all_parts[i]
        # set index to browser and ns_utc
        df = df.set_index([0,2])
        # delete visit column
        df.drop([1], axis=1, inplace=True)
        # leftjoin with original dataframe
        result = pd.concat([result, df], axis=1, join_axes=[result.index])
    # write end result to csv with 0.0 for all missing values
    result.to_csv(folder_out + "/" + slice + "cm.tsv", header=None, sep="\t", na_rep='0.0' )

# print time for performance test
st = time.strftime('%H:%M:%S', time.gmtime(time.time()-start_time))
print "Scriptlaufzeit: ", st