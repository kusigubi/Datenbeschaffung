# -*- coding: utf-8 -*-

file_name = "Sample4_merged/Events_Merged.tsv"
browser = "000FCF7A2D34423509E831FCD9CB7852-cs62"

with open(file_name) as f:
    for line in f:
        if line.startswith(browser):
            print line