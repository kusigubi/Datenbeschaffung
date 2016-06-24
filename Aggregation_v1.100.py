# -*- coding: utf-8 -*-
"""
Created on Tue Mar 29 10:29:09 2016

@author: gublermr, burgerch
"""

import os
import csv
import pandas as pd
import numpy as np
import copy
import time
import datetime as dt
import logging
import logging.handlers

from numpy.core.multiarray import broadcast

import collapser
import churn_collapser
"""
v1.100
cb: included new way to churn, generates a seperate churn-file (events-output is still generated)
v1.92
cb: human readable dates in output filenames
cb: moved definition of analysis start & enddates to config section
v1.91
mg: ouputfile is only being opend once per run.
mg: Flag for single file mode introduced
mg: BAD Browser file now written as csv
mg: catching any error on collapsing
mg: added resume mode for single file version

v1.9
cbu: seperate timelogger for collapsing
cbu: csv format change to semicolon delimited

v1.8
mg: search_other_V2 refactored so it does what it should do. has to be tested for speed improvement for entire dataset
mg: When no bad brwosers were being written, the bad browser file is being deleted at the end of the process.
v1.7
cbu: collapsing is now an external Object: "collapser.py". This simplifies the code massively and 
     allows for independent coding on collapser and 
     on Aggragation and simplifies changes in collapsing (as long as the return value is respected)
v1.6
cbu: Bad Browser File now takes an error message
cbu: New Function to calculate churn implemented: churn_binary
cbu: churn_binary raises an error if installation date is before analysis start
cbu: moved config section to top for convenience
cbu: added a new timelogger for logging time per file separatly -> writes in timelog.csv (for easy analysis in excel)
v1.5
mg: brwosers which cause an exception while collapsing are now being written into the file "BAD_browser..."

What's new: v1.4
cbu: added basic logging functionality
cbu: output csv file has now a timestamp string in the name, so we don't overwrite old files
cbu: in progress: new churn method "binary_churn"

What's new: v1.3
???
"""
# Folgende Variablen müssen für jede Analyse angepasst werden:
# folder_sample = hier muss das Directory mit den Inputfiles angegeben werden
# csv_filename = muss nur dann angegeben werden, wenn ein einzelnes File eingelesen wird. Bei Batchverarbeitung eines ganzen Folders werden alle Files der folder_sample Variable genommen
# output_filename = Bezeichnung des Ouputfiles
# header = Wenn das Report Builder Item angepasst wird, müssen hier die Spalten entsprechend angepasst werden.
# Je nach dem ob ein einzelnes File eingelesen wird oder im Batchmodus eingelesen wird, muss die entsprechende Methode aufgerufen werden (read_single_file oder multiple_files).
#
# in der Collapse Methode:
# analysis_start = Start des Analysezeitraums (wird für die Berechnung des Churns benötigt)
# analysis_end = Ende des Analysezeitraums (wird für die Berechnung des Churns benötigt)

""" CONFIG SECTION """
version = "1.100" # Versionnierung (v.a. fürs log)
# Pfad zum Verzeichnis mit den extrahierten Daten -> individuell anpassen
folder2 = '.' #Verzeichnis des Scripts
folder_sample = 'Sample4_merged' # Verzeichnis der Inputdatens
single_file_name="Events_Merged.tsv"
comScore_import_version = 2
single_file_mode = False

analysis_end = dt.datetime(2016, 04, 30, 23, 59, 59)
analysis_start = dt.datetime(2014, 8, 1, 00, 00, 00)

# im Single file mode wird am diesem Browserwert weiter collapsed
# resume_browser = '8D8AE1CAA9625EEB9B3D51D3945AA8CB-cs62'
resume_browser = '' #leave empty to go through all browsers
# file, in welches geschrieben werden soll (mode=append)
# resume_file = 'Event_Collapsed_1464254588.csv'
resume_file = '' #eave empty to go through all files


LOG_FILENAME = 'collapse_'+version+'.log'
LOGLEVEL = logging.INFO # DEBUG, INFO, or WARNING
""" END CONFIG SECTION """

# Ablauf des Skripts:
# Schritt 1:
# aus dem Inputfile wird ein Pandas DataFrame mit allen Visits erstellt (visit_df).
#
# Schritt 2:
# Die Events im Visits DataFrame (visit_df) werden mit der Collapse Methode auf eine Zeile verdichtet (single_visit_df)
# Die verdichteten Visits werden in das Outputfile geschrieben

def collapse(visit_df, output_file, first, bad_browser_file, analysis_start, analysis_end):
    start_collapsing_time = time.time()
    # Warnung für chained errors ausschalten
    pd.options.mode.chained_assignment = None
    if type(analysis_start) is not dt.datetime:
        raise TypeError('Analysis start must be a datetime.date, is ', type(analysis_start))
    if type(analysis_end) is not dt.datetime:
        raise TypeError('Analysis end must be a datetime.date, is ', type(analysis_end))
    if analysis_start > analysis_end:
        raise ValueError('Analysis start > Analysis end')
    
    try:
        """
        Collapser_object = collapser.Collapser(visit_df, start_collapsing_time, analysis_start, analysis_end)
        single_visit_df = Collapser_object.collapse()
        columns_order = Collapser_object.columns_order()
        """
        Collapser_object = churn_collapser.Churn_collapser(visit_df, start_collapsing_time, analysis_start, analysis_end)
        single_visit_df = Collapser_object.churn()
        columns_order = Collapser_object.columns_order()
        # Outputfile schreiben.
        single_visit_df.to_csv(path_or_buf=output_file, sep=';', header=first, index=False, quoting=csv.QUOTE_NONE, escapechar='\\', mode='a', columns=columns_order)

    except ValueError as ve:
        print("ValueError on collapsing: " + str(ve))
        log.error("ValueError on collapsing: " + str(ve))
        log_bad_browser(visit_df, bad_browser_file, ve)
        return
    except TypeError as te:
        print("ValueError on collapsing: " + str(te))
        log.error("TypeError on collapsing: " + str(te))
        log_bad_browser(visit_df, bad_browser_file, te)
        return
    except:
        print("Some Error on collapsing ")
        log.error("Some Error on collapsing: ")
        log_bad_browser(visit_df, bad_browser_file)
        return

    #Churner_object = churn_collapser.Churn_collapser(visit_df, start_collapsing_time, analysis_start, analysis_end)
    #churns_df = Churner_object.churn()
    #ccolumns_order = Churner_object.columns_order()
    #print churns_df.values
    #churns_df.to_csv(output_file, ';', first, False, csv.QUOTE_NONE, '\\', 'a', ccolumns_order)

    log.info("--collapsing took %s seconds!", time.strftime('%H:%M:%S', time.gmtime(time.time()-start_collapsing_time)))

def log_bad_browser(visit_df, badbrowser_file, error="default message"):
    badbrowser_file.write(visit_df["Browsers"][0] + ";" + str(error)+"\n")

def read_single_file(csv_filename, output_filename, header, bad_browser_file, analysis_start, analysis_end, resume_browser=''):
    last_browser=""
    first = True
    firstrun = True

    visit_df = pd.DataFrame(data=np.zeros((0,len(header))), columns=header)
    with open(csv_filename, 'r') as f:

        # Headerzeile suchen
        #line = f.readline().split('\t')
        #while line[0]<>'\"Browsers\"':
        #   line = f.readline().split('\t')

        # Alle Browser-IDs suchen bis nächste ID kommt
        for line in f:
            if len(line) == 1:
                continue
            line = line.split('\t')
            browser = line[0]
            if resume_browser <>'' and browser <= resume_browser:
                continue
            if browser <> last_browser and firstrun==False:
                # print "going to collapse browser", browser
                collapse(visit_df, output_filename, first, bad_browser_file, analysis_start, analysis_end)
                first = False
                visit_df = pd.DataFrame(data=np.zeros((0,len(header))), columns=header)
            else:
                firstrun = False
            line = [word.replace('\"','') for word in line]
            visit_df.loc[len(visit_df)]=line
            last_browser = browser


def read_multiple_files(file_list, output_filename, header, bad_browser_file, analysis_start, analysis_end):
    # File Dictionary erstellen mit den positionen der Zeilen
    #file_dict = {}
    #for file in file_list:
        #read_fileline_index(file, file_dict)
    
    browser_dict = {} # Browser-Dictionary (ID's aus comScore) initialisieren
    used_files_list = [] #liste durchsuchter files
    first_line = True

    last_browser="" #store last browser to know when a line belongs to a new browser
    visit_df = pd.DataFrame(data=np.zeros((0,len(header))), columns=header) #intialize Panda DataFrame with Headers as defined below
    for file_in in file_list:
        start_readfile_time = time.time()
        log.debug("starting out with file %s", file_in)
        csv_filename = os.path.join(folder_sample,file_in)
        used_files_list.append(file_in) #keep track of done files
        with open(csv_filename, 'r') as open_file:
            # Festlegen welche anderen Files noch durchsucht werden müssen
            other_files = copy.deepcopy(file_list) #Kopie der Originalliste
            # Files, welche bereits als Primärfile durchsucht wurden, müssen nicht mehr durchsucht werden.
            for removers in used_files_list:
                if other_files.count(removers)>0:
                    other_files.remove(removers)

            # alle files welche noch durchsucht werden müssen, öffnen.
            other_opend_files=[]
            for f in other_files:
                csv_filename = os.path.join(folder_sample,f)
                other_opend_files.append(open(csv_filename, "r"))

            firstrun = True 
            visit_df = pd.DataFrame(data=np.zeros((0,len(header))), columns=header) # cbu: do we need this initialization?
            last_line_dict = {}
            # alle Zeilen im File durchsuchen
            for line in open_file:
                line = line.split('\t') #dateien werden tab-delimited aus comScore exportiert (könnte evtl, als variable übergeben werden, zwecks portierbarkeit). LIST

                # Falls leere Zeilen vorhanden sind, Exception vermeiden. Wir rechnen nur mit leeren Zeilen am Ende eines Files->break ist gerechtfertigt wenn auch nicht schön, next file 
                if len(line)==1:
                    break

                # Browser auslesen
                actual_browser = line[0]

                #w wenn wir auf einen Browser treffen:
                if actual_browser <> last_browser and firstrun==False:
                    log.debug("now collecting browser: %s", last_browser)
                    # wenn der gefundene Browser nicht dem letzten entspricht, werden erst mal alle übrigen files durchsucht
                    #search_other(other_files, visit_df, last_browser) #go collect all visits to said browser
                    search_other_V2(other_opend_files, visit_df, last_browser, last_line_dict)
                    # dann wird der letzte browser (last_browser) collapsed und ein neuer erstellt
                    if browser_dict.has_key(last_browser)==False:
                        log.debug("collapsing browser: %s", visit_df["Browsers"][0])
                        # print "going to collapse", last_browser
                        collapse(visit_df, output_filename, first_line, bad_browser_file, analysis_start, analysis_end)
                        first_line = False
                        browser_dict[visit_df["Browsers"][0]]= "found"
                    visit_df = pd.DataFrame(data=np.zeros((0,len(header))), columns=header)
                    visit_df.loc[len(visit_df)]=line
                else:
                    #Zeile im DF mit Visits speichern
                    visit_df.loc[len(visit_df)]=line
                last_browser = actual_browser
                firstrun = False
            # letzter Browser muss noch ganz abgearbeitet werden
            if browser_dict.has_key(last_browser)==False:
                log.debug("collecting last browser: %s", last_browser)
                #search_other(other_files, visit_df, last_browser)
                search_other_V2(other_opend_files, visit_df, last_browser, last_line_dict)
                log.debug("collapsing last browser in file: %s", visit_df["Browsers"][0])
                print "going to collapse last browser in file", last_browser
                collapse(visit_df, output_filename, first_line, bad_browser_file, analysis_start, analysis_end)
                browser_dict[visit_df["Browsers"][0]]= "found"
        timelogger.info("run %s;readfile; %s ;%s", start_time, file_in, time.strftime('%H:%M:%S', time.gmtime(time.time()-start_readfile_time)))

        for f in other_opend_files:
            f.close()

def search_other(other_files, visit_df, browser_tofind):
    for file_in in other_files:
        csv_filename = os.path.join(folder_sample,file_in);
        log.debug("searching for browser %s in file %s", browser_tofind, csv_filename)
        with open(csv_filename, 'r') as f:
            for line in f:
                line = line.split('\t')
                if len(line)==1:
                    break
                browser = line[0]
                if browser == browser_tofind:
                    visit_df.loc[len(visit_df)]=line
                else:
                    if browser < browser_tofind:
                        break

# Methode, welche die zusätzlichen Files immer ab der Stelle weter durchsucht, an welcher aufgehört wurde.
# Ist leider langsamer als die Brute Force Methode, bei welcher jedes mal am Anfange wieder gestartet wird.
# Liegt daran, dass nur mit read_line() gearbeitet werden kann und das deutlich ineffizienter ist als wenn nur read() gemacht wird.
def search_other_V2(other_open_files, visit_df, browser_tofind, last_line_dict):
    for file_in in other_open_files:
        logging.debug("searching for browser %s in file %s", browser_tofind, file_in.name)
        # letzte gelesene Zeile des Files verarbeiten
        if last_line_dict.has_key(file_in.name):
            for line in last_line_dict[file_in.name]:
                if line[0]==browser_tofind:
                    visit_df.loc[len(visit_df)]=line
        for line in iter(file_in.readline, ''):
            line = line.split('\t')
            if len(line)==1:
                break
            browser = line[0]
            if browser == browser_tofind:
                visit_df.loc[len(visit_df)]=line
            else:
                if browser < browser_tofind:
                    # fälschlich ausgelesen Zeile zu den noch zu verarbeitenden Zeilen hinzufügen
                    if last_line_dict.has_key(file_in.name):
                        last_line_dict[file_in.name].append(line)
                    else:
                        last_line_dict[file_in.name]=[line]
                    break


# Zeitmessung einschalten
# abschätzen, wie lange das Skrip laufen würde bei ganzer Datenmenge
start_time = time.time()

# Logging aktivieren
log = logging.getLogger('standard_logger')
log.handlers = []
log_handler = logging.FileHandler(LOG_FILENAME, mode='a')
log.addHandler(log_handler)
log.setLevel(LOGLEVEL)
log.info("-----------------STARTING TO COLLAPSE v%s: %s'", version, dt.datetime.now())

timelogger = logging.getLogger('time_logger')
timelogger.handlers = []
timelogger_handler = logging.FileHandler('timelog.csv', mode='a')
timelogger.addHandler(timelogger_handler)
timelogger.setLevel(LOGLEVEL)

# csv_filename = os.path.join(folder2,'Event_Export_v2.csv') # cbu: i think we don't need this?
if resume_file <> "":
    output_filename = resume_file
else:
    #output_filename = "Event_Collapsed_" + format(start_time, '.0f') + ".csv"
    output_filename = "Event_Collapsed_" + dt.datetime.utcfromtimestamp(start_time).strftime("%Y-%m-%d %H%M") + ".csv"
    churns_filename ="Superchurns_" + dt.datetime.utcfromtimestamp(start_time).strftime("%Y-%m-%d %H%M") + ".csv"
output_filename = os.path.join(folder2, churns_filename)

# files für fehlerhafte Browser erstellen
bad_browser_filename="BAD_browser_file_"+version+"_"+dt.datetime.utcfromtimestamp(start_time).strftime("%Y-%m-%d %H%M")+".txt"
bad_browser_filename = os.path.join(folder2, bad_browser_filename)

# Bad Browser File öffnen
bad_browser_file = open(bad_browser_filename, 'w')

#neues Outputfile generieren
ft = open(output_filename , 'w')
sc = open(churns_filename, 'w')


# Liste mit allen Files im Verzeichnis erstellen
file_list = os.listdir(folder_sample)

if comScore_import_version == 1:
    header = ["Browsers","Visits","ns_ap_gs","ns_utc","Platform","Device OS","Manufacturer","Device","Operating system","Country","City","ns_radio","ns_ap_lastrun","ns_ap_updated","ns_ap_ver","Foreground_time","Background_time","Application starts cold","Application starts warm","Übersicht","Meine Favoriten","Such Resultate","Radar","Prognose Schweiz","Schnee","Wetterbericht","Meteo News","Über uns","Warnungen Warncenter","Impressum","Widget Add","Widget click","App install","App open","Landingpage","Karte","Artikel","Add Favorite"]
elif comScore_import_version == 2: #without ns_ap_lastrun, ns_updated, city 28.4.2016
    header = ["Browsers","Visits","ns_ap_gs","ns_utc","Platform","Device OS","Manufacturer","Device","Operating system","Country","ns_radio","ns_ap_ver","Foreground_time","Background_time","Application starts cold","Application starts warm","Übersicht","Meine Favoriten","Such Resultate","Radar","Prognose Schweiz","Schnee","Wetterbericht","Meteo News","Über uns","Warnungen Warncenter","Impressum","Widget Add","Widget click","App install","App open","Landingpage","Karte","Artikel","Add Favorite"]
else:
    raise ValueError('Header not fitting the raw data!') # that's how we'll handle it later on

# Switch between single file or multiple file mode.
if single_file_mode:
    read_single_file(os.path.join(folder_sample,single_file_name), sc, header, bad_browser_file, analysis_start, analysis_end, resume_browser)
else:
    read_multiple_files(file_list, sc, header, bad_browser_file, analysis_start, analysis_end)

# output file und bad browser file schliessen
bad_browser_file.close()
ft.close()
sc.close()

# wenn bad browser file leer ist, wird das file wieder gelöscht.
file_status = os.stat(bad_browser_file.name)
if file_status.st_size == 0:
    os.remove(bad_browser_file.name)

# Zeitmessung ausgeben
st = time.strftime('%H:%M:%S', time.gmtime(time.time()-start_time))
timelogger.info("run %s; totalruntime; ; %s", start_time, time.strftime('%H:%M:%S', time.gmtime(time.time()-start_time)))
log.info("-----------------ENDING COLLAPSE: runtime: %s", st)
print "Scriptlaufzeit: ", st