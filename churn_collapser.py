# -*- coding: utf-8 -*-
"""
v1.4
mg: variables sessions_regelmaess_*Zeitraum* added
mg: test for division by 0 for mobility rate
mg: last_visit introduced
mg: bug corrected in the visits_dauer_[timerange], visits_regel_[timerange] and visits_[timerange]_h variables
mg: instroduced the variables binary_inactivity_60_d, binary_inactivity_90_d, number_visits_after_li, last_visit_before_li

v1.1
mg: Removal of calculation errors
mg: introduction of visits_h variable: Number of visits at a given hour of day
mg: churn method debugged
mg: sorting debugged

"""
"""
Created on Sat Apr 23 11:42:46 2016
# Collapsinig Objekt für ein DataFrame mit Visits
# Schritt 1:
# Die Visits im visit_df werden chronologisch sortiert
# Datumswerte werden zu solchen konvertiert
#
# Schritt 2:
# Das Output DataFrame (single_visit_df) wird erstellt und mit dem Header initialisiert. Sämtliche Variablen, welche im Output benötigt werden, müssen im Header initialisiert werden.
# Die Reichenfolge der Outputspalten wird festgelegt.
#
# Schritt 3:
# Collapsing der Variablen über alle Visits.
# 1. Fixe Werte über alle Visits ausgeben
# 2. Aggregierte Daten über alle Visits (mean Visit duration usw.). Immer 1 Wert als output
# 3. Churnvariablen berechnen. Mittels calc_churn Methode berechnen, ob ein entprechender Churn eingetroffen ist oder nicht.
#
# Schritt 4:
# single_visit_df in den Output schreiben.

Attributes
----------
log: logger
feature_list: list (containing all features of the SRF MeteoApp as defined by srg_n1)
analysis_end: datetime (End of Analysis Period)
analysis_start: datetime (Start of Analysis Period)
visit_df: Panda Dataframe (containing a matrix of all visits belonging to a given browser)
columns_order: List (columns as definden by visit_df)
timerange_list: List (as required by Datenauswertungs-Team)
returnvalues_default: List (defaultvalues for all columns in visit_df, in case some values are empty)

Functions
---------
define_defaults(): List (containing default values for all columns in visit_df)
sort_visits(): None (sorts visits by timestamp)
format_visits(): None (converts Number-Strings to Numbers and DateTime-String to datetime-objects (or numpy Timestamp objects in reality))
churn_binary(self, visit_df, analysis_start, max_days=0, churn_delay=7, churn_days=30): 
    Timedate (Last Visit before churn if a churn is found)
    Bool (False if none is found)
collapse(): Panda Dataframe (containing a row with the collased values belonging to a certain Browser)

@author: gublermr, burgerch
"""
import pandas as pd
import numpy as np
import time
import datetime as dt
import logging

class Churn_collapser(object):
    
    def __init__(self, visit_df, aggregator_start_time, analysis_start, analysis_end):
        self.aggregator_start_time = aggregator_start_time #for logging reasons
        self.log = logging.getLogger('standard_logger')
        self.collapsetimelogger = logging.getLogger('time_logger')
        #print("New Collapser for ", visit_df["Browsers"][0], ";" , len(visit_df))
        if visit_df.size == 0:
            raise ValueError('Visit DF for Browser : '+visit_df["Browsers"][0]+" was empty!")

        self.analysis_end = analysis_end
        self.analysis_start = analysis_start
        self.visit_df = visit_df
        self.installation_timestamp = self.visit_df['ns_ap_gs'][0]
        # Zeiträume
        self.churnstart_list = [] #this is new and replacing the timerange list to avaoid confusion
        for x in range(20, 140, 10):
            self.churnstart_list.append(x)
        self.churnduration_list= []
        for x in range(20, 140, 10):
            self.churnduration_list.append(x)
        
        self.returnvalues_default = self.define_defaults()
        # self.sort_visits()
        self.format_visits()
        print "---------------------------------New collapser for:", visit_df["Browsers"][0]
                
    def define_defaults(self):
        # Default Werte für alle Spalten. Muss gegebenenfalls als Zahl definiert werden. Bis jetzt alles einfach String...
        defaultvalues = {"Browsers":[""],"Visits":[""],"ns_ap_gs":[""], "longest_inactivity":[""], "number_visits_after_li":[""], "last_visit_before_li":[""]}

        for cs in self.churnstart_list:
            for cd in self.churnduration_list:
                defaultvalues["churn_start"+str(cs)+"_duration"+str(cd)]="False"
                defaultvalues["churn_start" + str(cs) + "_duration" + str(cd) + "_visits"] = 0
                defaultvalues["churn_start" + str(cs) + "_duration" + str(cd) + "_effdays"] = 0
        return defaultvalues
        
    def sort_visits(self): # Visits chronologisch sortieren
        start_time = time.time()
        self.visit_df = self.visit_df.sort(['ns_utc'], ascending=True)
        self.collapsetimelogger.info("run %s; sorting; %s; %s",self.aggregator_start_time, len(self.visit_df), time.strftime('%H:%M:%S', time.gmtime(time.time()-start_time)))

    """
    Copy ns_ap_gs (installation time) to all visit rows (not just first one)
    Transform timestamps (default in miliseconds from comScore) to second-values (default in py-datetime)
    """
    def format_visits(self):
        self.visit_df=self.visit_df.convert_objects(convert_numeric=True)
        #print visit_df["ns_ap_gs"]
        for i in range(len(self.visit_df)):
            self.visit_df['ns_ap_gs'][i] = self.visit_df['ns_ap_gs'][0]
        self.visit_df['ns_ap_gs'] = [dt.datetime.fromtimestamp(t/1000) for t in self.visit_df.ns_ap_gs]
        self.visit_df['ns_utc'] = [dt.datetime.fromtimestamp(t/1000) for t in self.visit_df.ns_utc]
        
    def columns_order(self):
        # Die hier definierte Reihenfolge wird in den Output geschrieben. Wenn oben eine neue Spalte definiert wird, muss sie auch für den export definiert werden
        colums_order = ["Browsers","Visits","ns_ap_gs", "longest_inactivity", "number_visits_after_li", "last_visit_before_li"]
        
        for cs in self.churnstart_list:
            for cd in self.churnduration_list:
                colums_order.append("churn_start" + str(cs) + "_duration" + str(cd))
                colums_order.append("churn_start" + str(cs) + "_duration" + str(cd) + "_visits")
                colums_order.append("churn_start" + str(cs) + "_duration" + str(cd) + "_effdays")
        return colums_order

    """ Da berechnen wir mal den Churn über die verschiedenen Zeiten grundsätzlich neu!!"""
    """ Berechnet, ob ein Churn eingetroffen ist. Falls eine bestimmte churn-bedingung eingetroffen ist, wird
    wird das Datum des letzten Visits eingetragen. Falls kein Churn eingetroffen ist, False !!

    Parameters
    ----------
    visit_df : Panda Dataframe (containing a row with all variables for the output)
    analysis_start: Datetime (Start of the Analysis)

    Returns
    -------
    churns_df : Panda Dataframe for all possible churn definitions, with:
        Timedate Last Visit before churn if a churn is found
        Bool False if none is found
    """
    def churn(self):
        self.sort_visits()
        start_time = time.time()
        churns_df = pd.DataFrame(self.returnvalues_default)
        installation_time = self.visit_df["ns_ap_gs"][0]
        last_visit = installation_time  # der erste visit ist immer das installationsdatum! (def comScore)
        max_duration = self.churnduration_list[-1]
        max_startdays = self.churnstart_list[-1]

        # Fixe Werte über alle Visits ausgeben
        churns_df["Browsers"][0] = self.visit_df["Browsers"][0]
        churns_df["Visits"][0] = len(self.visit_df)
        churns_df["ns_ap_gs"][0] = self.visit_df["ns_ap_gs"][0]

        # longest_inactivity
        last = self.visit_df["ns_utc"].iloc[0]
        max_inact = dt.timedelta(hours=0)
        last_visit = 0
        for i in range(1, len(self.visit_df)):
            # print self.visit_df["ns_utc"].iloc[i]
            if self.visit_df["ns_utc"].iloc[i] - last > max_inact:
                max_inact = self.visit_df["ns_utc"].iloc[i] - last
                churns_df["last_visit_before_li"].iloc[0] = last
                churns_df["number_visits_after_li"].iloc[0] = len(self.visit_df) - i
            last = self.visit_df["ns_utc"].iloc[i]
            churns_df["longest_inactivity"].iloc[0] = max_inact.total_seconds()

        if self.analysis_start > installation_time:  # wenn die App vor dem Analysestart installiert wurde -> browser ausschliesen
            raise ValueError('Browser was installed before Analysis Start: ' + str(installation_time) + "!")  # that's how we'll handle it later on
            # installation_time = analysis_start
        if self.analysis_end - installation_time < dt.timedelta(days=(max_startdays + max_duration)):
            raise ValueError('Browser was is too young to analyse (< max_startdays + max_duration): ' + str(installation_time))
        rows = len(self.visit_df.index)
        last_visit = installation_time
        for current_row in range(0, rows):
            current_visit = self.visit_df["ns_utc"].iloc[current_row]
            inactivity = current_visit - last_visit
            for churn_timerange in self.churnstart_list:
                #print("checking", self.visit_df["Browsers"].iloc[i], "installed at", str(installation_time), "at", str(current_visit), "=", str(installedsince.days), ">", churn_timerange)
                if (current_visit > installation_time + dt.timedelta(days=churn_timerange)):  # if visit is before churnstart
                    continue
                for churn_duration in self.churnduration_list:
                    #print("checking churn after", churn_timerange, "days, for", churn_duration, "days duration (", str(current_visit),")")

                    if (inactivity > dt.timedelta(days=churn_duration) and churns_df["churn_start" + str(churn_timerange) + "_duration" + str(churn_duration)][0] == "False"):
                        # print("found churn: ", current_visit - last_visit, " last visit: ", last_visit)
                        churns_df["churn_start" + str(churn_timerange) + "_duration" + str(churn_duration)][0] = last_visit
                        churns_df["churn_start" + str(churn_timerange) + "_duration" + str(churn_duration)+ "_visits"][0] = rows - current_row
                        churns_df["churn_start" + str(churn_timerange) + "_duration" + str(churn_duration) + "_effdays"][0] = (current_visit - last_visit).days
            last_visit = current_visit

        # check if last visit is more than churn time away from analysis end
        current_visit = self.visit_df["ns_utc"].iloc[rows - 1]
        #print "Handling last Visit:", str((self.analysis_end - current_visit).days), "days away from analysis_end and ", str((current_visit - installation_time).days), "days away from installation_time"
        for churn_timerange in self.churnstart_list:
            if current_visit > installation_time + dt.timedelta(days=churn_timerange):
                continue
            for churn_duration in self.churnduration_list:
                inactivity = self.analysis_end - current_visit
                if inactivity > dt.timedelta(days=churn_duration) and churns_df["churn_start" + str(churn_timerange) + "_duration" + str(churn_duration)][0]=="False":
                    # print("found churn in start", churn_timerange, "duration", churn_duration, ":", self.analysis_end - current_visit, " last visit: ", current_visit)
                    churns_df["churn_start" + str(churn_timerange) + "_duration" + str(churn_duration)][0] = current_visit
                    churns_df["churn_start" + str(churn_timerange) + "_duration" + str(churn_duration) + "_visits"][0] = 0
                    churns_df["churn_start" + str(churn_timerange) + "_duration" + str(churn_duration) + "_effdays"][0] = (self.analysis_end - current_visit).days

        self.collapsetimelogger.info("run %s; collapsing; %s; %s", self.aggregator_start_time, len(self.visit_df), time.strftime('%H:%M:%S', time.gmtime(time.time() - start_time)))
        return churns_df
