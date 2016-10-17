# -*- coding: utf-8 -*-
"""
Created on Mon May 2 8:35:00 2016

@author: gublermr
"""
import os
import time
import heapq

"""
v1.0
MG: first test for the heapq method, in oderder to merge all exported files into 1 file

"""
"""
The script merges multiple files which are sorted ascending into one file
To adapt:
folder: folder of all input files
folder_out: folder in which the output has to be written
"""


# Zeitmessung einschalten
# abschätzen, wie lange das Skrip laufen würde bei ganzer Datenmenge
start_time = time.time()
folder = "Sample4_crossval"
folder_out = "Sample4_crossval_merged"

# csv_filename = os.path.join(folder2,'Event_Export_v2.csv') # cbu: i think we don't need this?
output_filename = "Events_Merged.tsv"
output_filename = os.path.join(folder_out, output_filename)


#neues Outputfile generieren
ft = open(output_filename , 'w')


# Liste mit allen Files im Verzeichnis erstellen
file_list = os.listdir(folder)
open_file_list = []
for files in file_list:
    open_file_list.append(open(os.path.join(folder, files),"r"))

ft.writelines(heapq.merge(*open_file_list))

for open_file in open_file_list:
    open_file.close()

ft.close()

# Zeitmessung ausgeben
st = time.strftime('%H:%M:%S', time.gmtime(time.time()-start_time))
print "Scriptlaufzeit: ", st