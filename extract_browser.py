# -*- coding: utf-8 -*-

file_name = "Sample4_redef_start_unrestriced/Events_Merged.tsv"
browser = "9D52FADA8988E7DBAD67FC703FA9A09D-cs62"

with open(file_name) as f:
    for line in f:
        if line.startswith(browser):
            print str.strip(line, "\n")