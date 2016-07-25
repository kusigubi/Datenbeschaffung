# -*- coding: utf-8 -*-

file_name = "Sample4_merged/Events_Merged.tsv"
browser = "0222e1fa2c45c7e869a1a3b81e9aa2ab-cs31"

with open(file_name) as f:
    for line in f:
        if line.startswith(browser):
            print str.strip(line, "\n")