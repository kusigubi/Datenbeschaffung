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
import glob

from numpy.core.multiarray import broadcast

import collapser
import churn_collapser
"""
What's new: v1.0
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
version = "0.1" # Versionnierung (v.a. fürs log)
# Pfad zum Verzeichnis mit den extrahierten Daten -> individuell anpassen
folder2 = '.' #Verzeichnis des Scripts
folder_sample = 'Sample4_redef_start_unrestriced' # Verzeichnis der Inputdatens
#folder_sample = 'Sample_dv'
single_file_name="Events_Merged.tsv"
comScore_import_version = 2
#comScore_import_version = 1
single_file_mode = False
filetype="tsv"


analysis_end = dt.datetime(2016, 04, 30, 23, 59, 59)
analysis_start = dt.datetime(2014, 8, 1, 00, 00, 00)

# im Single file mode wird am diesem Browserwert weiter collapsed
# resume_browser = '8D8AE1CAA9625EEB9B3D51D3945AA8CB-cs62'
resume_browser = '' #leave empty to go through all browsers
# file, in welches geschrieben werden soll (mode=append)
# resume_file = 'Event_Collapsed_1464254588.csv'
resume_file = '' #eave empty to go through all files


LOG_FILENAME = 'featureoverlap_'+version+'.log'
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

        Collapser_object = collapser.Collapser(visit_df, start_collapsing_time, analysis_start, analysis_end)
        single_visit_df = Collapser_object.collapse()
        columns_order = Collapser_object.columns_order()
        """
        Collapser_object = churn_collapser.Churn_collapser(visit_df, start_collapsing_time, analysis_start, analysis_end)
        single_visit_df = Collapser_object.churn()
        columns_order = Collapser_object.columns_order()
        """
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
    except Exception as ex:
        print("Some Error on collapsing " + str(ex))
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
        #if os.path.isfile(file_in):
        #    print "isfile:", file_in
        start_readfile_time = time.time()
        log.debug("starting out with file %s", file_in)
        #csv_filename = os.path.join(folder_sample,file_in)
        csv_filename = file_in
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
timelogger_handler = logging.FileHandler('collapse_timelog.csv', mode='a')
timelogger.addHandler(timelogger_handler)
timelogger.setLevel(LOGLEVEL)

# csv_filename = os.path.join(folder2,'Event_Export_v2.csv') # cbu: i think we don't need this?
if resume_file <> "":
    output_filename = resume_file
else:
    #output_filename = "Event_Collapsed_" + format(start_time, '.0f') + ".csv"
    output_filename = "Event_Collapsed_" + dt.datetime.utcfromtimestamp(start_time).strftime("%Y-%m-%d %H%M") + ".csv"
    output_directory = os.path.join(folder_sample, "collapsed")
output_filename = os.path.join(output_directory, output_filename)

# files für fehlerhafte Browser erstellen
bad_browser_filename="BAD_browser_file_"+version+"_"+dt.datetime.utcfromtimestamp(start_time).strftime("%Y-%m-%d %H%M")+".txt"
bad_browser_directory = os.path.join(folder_sample, "bad_browsers")
bad_browser_filename = os.path.join(bad_browser_directory, bad_browser_filename)

# Bad Browser File öffnen
if not os.path.exists(bad_browser_directory):
    os.makedirs(bad_browser_directory)
bad_browser_file = open(bad_browser_filename, 'w')

#neues Outputfile generieren
if not os.path.exists(output_directory):
    os.makedirs(output_directory)
ft = open(output_filename, 'w')

# Liste mit allen Files im Verzeichnis erstellen
file_list = glob.glob(folder_sample + "/*."+filetype)
print file_list

if comScore_import_version == 1:
    header = ["Browsers","Visits","ns_ap_gs","ns_utc","Platform","Manufacturer","Device","Operating system","Country","ns_radio","ns_ap_ver","Foreground_time","Feature Search Radio","Feature Search TV", "Feature Favorites", "Feature TV Overview", "Feature TV By Date", "Feature TV A-Z", "Feature TV Now on TV", "Feature Radio Channel 1", "Feature Radio Channel 2", "Feature Radio Channel 3", "Feature Radio Channel 4", "Feature Radio Channel 5", "Feature Radio Channel 6", "Visits", "TV Channel 1 Streaming Starts", "TV Channel 1 Streaming Duration", "TV Channel 2 Streaming Starts", "TV Channel 2 Streaming Duration", "TV Channel 3 Streaming Starts", "TV Channel 3 Streaming Duration",
              "TV VoD Streaming Starts", "TV VoD Streaming Duration", "Radio Channel 1 Streaming Starts", "Radio Channel 1 Streaming Duration", "Radio Channel 2 Streaming Starts", "Radio Channel 2 Streaming Duration", "Radio Channel 3 Streaming Starts", "Radio Channel 3 Streaming Duration", "Radio Channel 4 Streaming Starts", "Radio Channel 4 Streaming Duration", "Radio Channel 5 Streaming Starts", "Radio Channel 5 Streaming Duration", "Radio Channel 6 Streaming Starts", "Radio Channel 6 Streaming Duration", "Radio AoD Streaming Starts", "Radio AoD Streaming Duration", "Content Group Unknown Starts", "Content Group Unknown Duration", "Content Group Sport Starts", "Content Group Sport Duration",
              "Content Group Comdey Starts", "Content Group Comedy Duration", "Content Group Documentary Starts", "Content Group Documentary Duration", "Content Group Society Politics Starts", "Content Group Society Politics Duration", "Content Group Films Series Starts", "Content Group Films Series Duration", "Content Group Education Starts", "Content Group Education Duration", "Content Group Culture Religion Starts", "Content Group Culture Religion Duration", "Content Group Kids Teens Starts", "Content Group Kids Teens Duration", "Content Group News Economy Starts", "Content Group News Economy Duration", "Content Group Music Starts", "Content Group Music Duration", "Content Group Consumer Starts", "Content Group Consumer Duration",
              "Web Only Starts", "Number of Episodes", "Feature Share", "Feature Share Social", "Feature Favorites Marked", "nr of C1 Values", "Chromecast or Airplay", "geoblocked view"]
elif comScore_import_version == 2: #without ns_ap_lastrun, ns_updated, city 28.4.2016
    header = ["Browsers","Visits","ns_ap_gs","ns_utc","Platform","Device OS","Manufacturer","Device","Operating system","Country","ns_radio","ns_ap_ver","Foreground_time","Background_time","Application starts cold","Application starts warm","Übersicht","Meine Favoriten","Such Resultate","Radar","Prognose Schweiz","Schnee","Wetterbericht","Meteo News","Über uns","Warnungen Warncenter","Impressum","Widget Add","Widget click","App install","App open","Landingpage","Karte","Artikel","Add Favorite"]
else:
    raise ValueError('Header not fitting the raw data!') # that's how we'll handle it later on

# Switch between single file or multiple file mode.
if single_file_mode:
    read_single_file(os.path.join(folder_sample,single_file_name), ft, header, bad_browser_file, analysis_start, analysis_end, resume_browser)
else:
    read_multiple_files(file_list, ft, header, bad_browser_file, analysis_start, analysis_end)

# output file und bad browser file schliessen
bad_browser_file.close()
ft.close()

# wenn bad browser file leer ist, wird das file wieder gelöscht.
file_status = os.stat(bad_browser_file.name)
if file_status.st_size == 0:
    os.remove(bad_browser_file.name)

# Zeitmessung ausgeben
st = time.strftime('%H:%M:%S', time.gmtime(time.time()-start_time))
timelogger.info("run %s; totalruntime; ; %s", start_time, time.strftime('%H:%M:%S', time.gmtime(time.time()-start_time)))
log.info("-----------------ENDING COLLAPSE: runtime: %s", st)
print "Scriptlaufzeit: ", st