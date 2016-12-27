# -*- coding: utf-8 -*-
"""
Created on Tue Dec 27. 2016

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


# A single line from part 1 is being matched to the lines of part 2 and more.
# if in a file no corresponding line is being found, a the values are being retruned as "0"
def readLine(line, files):
    line = line.replace("\n","")
    line = line.split("\t")
    line_part = []
    print(line[0], line[3])
    # iterate through all files with part 2 or more
    for i in range(1, len(files)):
        file = files[i]
        file.seek(0)
        lenght = 0
        found = False
        # iterate through all lines in a part file
        for newline in file:
            newline = newline.replace("\n", "")
            newline = newline.split("\t")
            lenght = len(newline)
            if newline[0]==line[0] and newline[2]==line[3]:
                line_part.append(newline[3:])
                found = True
                continue
        # if the browser and ns_utc can't be found, a blank line with "0" is being returned
        if not found:
            blanks = []
            for i in range(1, lenght-2):
                blanks.append("0")
            line_part.append(blanks)
    for part in line_part:
        line = line + part
    return line

folder_in = "SRG Play App/sample one day"
folder_out= "SRG Play App/sample one day cm"

time_slice = []
part_slice = []
for name in os.listdir(folder_in):
    pos = name.find("part")
    if (name[pos:] not in part_slice):
        part_slice.append(name[pos:])
    if name[:pos] not in time_slice:
        time_slice.append(name[:pos])


# iterate through all time slices of the export
for slice in time_slice:
    file_out = open(folder_out+"/"+slice+"cm.tsv", "w")
    file_list = []
    for part in part_slice:
        print(slice+part)
        file_list.append(open(folder_in+"/"+slice+part, 'r'))
    first_file = file_list[0]
    for lines in first_file:
        line = readLine(lines, file_list)
        file_out.write('\t'.join(line)+"\n")

    for file_item in file_list:
        file_item.close()
file_out.close()
print(time_slice)
print(part_slice)