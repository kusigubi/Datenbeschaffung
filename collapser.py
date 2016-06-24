# -*- coding: utf-8 -*-
"""
v1.5



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

class Collapser(object):
    
    def __init__(self, visit_df, aggregator_start_time, analysis_start, analysis_end):
        self.aggregator_start_time = aggregator_start_time #for logging reasons
        self.log = logging.getLogger('standard_logger')
        self.collapsetimelogger = logging.getLogger('time_logger')
        #print("New Collapser for ", visit_df["Browsers"][0], ";" , len(visit_df))
        if visit_df.size == 0:
            raise ValueError('Visit DF for Browser : '+visit_df["Browsers"][0]+" was empty!")
            
        self.feature_list = ["Übersicht","Meine Favoriten","Such Resultate","Radar","Prognose Schweiz","Schnee","Wetterbericht","Meteo News","Über uns","Warnungen Warncenter","Impressum","Widget Add","Widget click","App install","App open","Landingpage","Karte","Artikel","Add Favorite"]
        self.analysis_end = analysis_end
        self.analysis_start = analysis_start
        self.visit_df = visit_df
        self.installation_timestamp = self.visit_df['ns_ap_gs'][0]
        # Zeiträume
        self.timerange_list = []
        for x in range(24, 337, 24):
            self.timerange_list.append(x)
        
        self.returnvalues_default = self.define_defaults()
        self.sort_visits()
        self.format_visits()
        print visit_df["Browsers"][0]
                
    def define_defaults(self):
        # Default Werte für alle Spalten. Muss gegebenenfalls als Zahl definiert werden. Bis jetzt alles einfach String...
        defaultvalues = {"Browsers":[""],"Visits":[""],"ns_ap_gs":[""],"Platform":[""],"Device":[""],"Manufacturer":[""],"Operating system":[""],"Country":[""],"ns_radio":0, "mobility_rate":0.0,"avg_session_duration": 0.0,"sd_session_duration": 0.0,"binary_churn": 0,"last_before_churn_time": [""],"churn_time":[""],"churn_30d": 0,"churn_60d": 0,"churn_90d": 0, "binary_inactivity_60_d":0,"binary_inactivity_90_d":0 , "number_visits_after_li":0, "last_visit_before_li":0,"installation_season":0,"installation_time":0,"installation_weekend":0,"longest_inactivity":0, "last_visit":0}
        
        # Automatisch generierte Variablen anhängen
        # Average session duration
        for x in range(24):
            defaultvalues["visit_h_" + str(x)]=0
            defaultvalues["avg_visit_duration_h_" + str(x)]=0
            defaultvalues["sd_visit_duration_h_" + str(x)]=0
        for f in self.feature_list:
            defaultvalues["visits_" + f]=0
        for t in self.timerange_list:
            defaultvalues["visits_"+str(t)+"_h"]=0
            defaultvalues["visits_dauer_"+str(t)+"_h"]=0
            defaultvalues["visits_regel_"+str(t)]=0
        for t in self.timerange_list:
            for f in self.feature_list:
                defaultvalues[f+"_"+str(t)+"_h"]=0
        
        return defaultvalues
        
    def sort_visits(self): # Visits chronologisch sortieren
        start_time = time.time()
        self.visit_df = self.visit_df.sort(['ns_utc'], ascending=True)
        self.collapsetimelogger.info("run %s; sorting; %s; %s",self.aggregator_start_time, len(self.visit_df), time.strftime('%H:%M:%S', time.gmtime(time.time()-start_time)))
        
    def format_visits(self):
        self.visit_df=self.visit_df.convert_objects(convert_numeric=True)
        #print visit_df["ns_ap_gs"]
        for i in range(len(self.visit_df)):
            self.visit_df['ns_ap_gs'][i] = self.visit_df['ns_ap_gs'][0]
        self.visit_df['ns_ap_gs'] = [dt.datetime.fromtimestamp(t/1000) for t in self.visit_df.ns_ap_gs]
        self.visit_df['ns_utc'] = [dt.datetime.fromtimestamp(t/1000) for t in self.visit_df.ns_utc]
        
    def columns_order(self):
        # Die hier definierte Reihenfolge wird in den Output geschrieben. Wenn oben eine neue Spalte definiert wird, muss sie auch für den export definiert werden
        colums_order = ["Browsers","Visits","ns_ap_gs","Platform","Device","Manufacturer","Operating system","Country","ns_radio","mobility_rate",  "avg_session_duration","sd_session_duration", "binary_churn","last_before_churn_time","churn_time", "churn_30d","churn_60d","churn_90d","binary_inactivity_60_d", "binary_inactivity_90_d", "number_visits_after_li", "last_visit_before_li", "installation_season", "installation_time", "installation_weekend", "longest_inactivity", "last_visit"]
        
        # Automatisch generierte Variablen anhängen
        # Average session duration
        for x in range(24):
            colums_order.append("visit_h_" + str(x))
        for x in range(24):
            colums_order.append("avg_visit_duration_h_" + str(x))
        for x in range(24):
            colums_order.append("sd_visit_duration_h_" + str(x))
        for f in self.feature_list:
            colums_order.append("visits_" + f)
        for t in self.timerange_list:
            colums_order.append("visits_" + str(t) + "_h")
        for t in self.timerange_list:
            colums_order.append("visits_dauer_" + str(t) + "_h")
        for t in self.timerange_list:
            colums_order.append("visits_regel_" + str(t))
        for t in self.timerange_list:
            for f in self.feature_list:
                colums_order.append(f+"_"+str(t)+"_h")
                
        return colums_order
        
    """ Berechnet, ob ein Churn eingetroffen ist. Falls Visits innerhalb der churn_time mehr als 30 Tage inaktiv ist, 
    wird das Datum des letzten Visits zurückgegeben. Falls kein Churn eingetroffen ist, False !!
    
    Parameters
    ----------
    visit_df : Panda Dataframe (containing a row with all variables for the output)
    analysis_start: Datetime (Start of the Analysis)
    max_days: Optional Int (Max days after installation to look at. 0 if no limit)
    churn_delay: Optional Int (Delay of days after installation to start looking for a churn. 7 is default)
    churn_days: Optional Int (How much days defines a churn. 30 is default)
    
    Returns
    -------
    Timedate Last Visit before churn if a churn is found
    Bool False if none is found
    """
    def churn_binary(self, visit_df, analysis_start, analysis_end, max_days=0, churn_delay=7, churn_days=30):
        installation_time = visit_df["ns_ap_gs"][0]
        last_visit = installation_time #der erste visit ist immer das installationsdatum! (def comScore)
        if analysis_start > installation_time: # wenn die App vor dem Analysestart installiert wurde -> browser ausschliesen
            raise ValueError('Browser was installed before Analysis Start: '+str(installation_time)+"!") # that's how we'll handle it later on
            # installation_time = analysis_start
        rows = len(visit_df.index)
        # scan all visits
        for x in range(0,rows):
            current_visit = visit_df["ns_utc"].iloc[x]

            # if visit is within churn delay offset (within 7 days after install), go to next visit to check.
            if (current_visit < installation_time + dt.timedelta(days=churn_delay)):
                last_visit = current_visit
                continue

            # if the scanning is limited to max days, the method returns false as soon as the visit is outside the max days boundary
            if max_days > 0:
                if current_visit - installation_time > dt.timedelta(days=max_days):
                    #self.log.debug("Churn within %s days after installation not found", max_days)
                    return False

            # as soon as 2 visits are more than the churn time range appart, the last visit before the churn is being returned
            if(current_visit - last_visit > dt.timedelta(days=churn_days)):
               #self.log.debug("found churn for browser %s: %s (%s - %s)", visit_df["Browsers"][0], current_visit - last_visit, last_visit, current_visit)
               return last_visit
            last_visit = current_visit

        # check if last visit is more than churn time away from analysis end
        current_visit = visit_df["ns_utc"].iloc[rows-1]
        if analysis_end - current_visit > dt.timedelta(days=churn_days):
            return visit_df["ns_utc"].iloc[len(visit_df.index)-1]

        # no churn found
        return False
    
    """
    Collapsinig methode für ein DataFrame mit Visits
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
    """    
    def collapse(self):
        start_time = time.time()
        single_visit_df = pd.DataFrame(self.returnvalues_default)
        
        # Fixe Werte über alle Visits ausgeben
        single_visit_df["Browsers"][0] = self.visit_df["Browsers"][0]
        single_visit_df["Visits"][0]   = len(self.visit_df)
        single_visit_df["ns_ap_gs"][0] = self.visit_df["ns_ap_gs"][0]
        if self.visit_df["Platform"][0] == "Mobile":
            single_visit_df["Platform"][0] = 0
        else:
            if self.visit_df["Platform"][0] == "Tablet":
                single_visit_df["Platform"][0] = 1
            else:
                single_visit_df["Platform"][0] = 2
        if self.visit_df["Operating system"][0].find("Android")>-1:
            single_visit_df["Operating system"][0] = 0
        else:
            if self.visit_df["Operating system"][0].find("iOS")>-1:
                single_visit_df["Operating system"][0] = 1
            else:
                single_visit_df["Operating system"][0] = 2
        single_visit_df["Manufacturer"][0]     = self.visit_df["Manufacturer"][0]
        single_visit_df["Device"][0] = self.visit_df["Device"][0]
        if self.visit_df["Country"][0].find("Switzerland")>-1:
            single_visit_df["Country"][0] = 0
        else:
            single_visit_df["Country"][0] = 1
        if self.visit_df["ns_ap_gs"][0].date().weekday() >4:
            single_visit_df["installation_weekend"][0] = 1
        else:
            single_visit_df["installation_weekend"][0] = 0

        if self.visit_df["ns_ap_gs"][0].date().month < 4:
            single_visit_df["installation_season"][0] = 1
        elif self.visit_df["ns_ap_gs"][0].date().month < 7:
            single_visit_df["installation_season"][0] = 2
        elif self.visit_df["ns_ap_gs"][0].date().month < 10:
            single_visit_df["installation_season"][0] = 3
        else:
            single_visit_df["installation_season"][0]=4

        if self.visit_df["ns_ap_gs"][0].time().hour < 6:
            single_visit_df["installation_time"][0] = 0
        elif self.visit_df["ns_ap_gs"][0].time().hour < 12:
            single_visit_df["installation_time"][0] = 1
        elif self.visit_df["ns_ap_gs"][0].time().hour < 14:
            single_visit_df["installation_time"][0] = 2
        elif self.visit_df["ns_ap_gs"][0].time().hour < 18:
            single_visit_df["installation_time"][0] = 3
        elif self.visit_df["ns_ap_gs"][0].time().hour < 22:
            single_visit_df["installation_time"][0] = 4
        else:
            single_visit_df["installation_time"][0] = 0

        # Aggregierte Daten über alle Visits
        # homeuser_binary
        if any(self.visit_df["ns_radio"] == "wifi"):
            single_visit_df["ns_radio"][0] = 1
        else:
            single_visit_df["ns_radio"][0] = 0

        # mobility_rate
        wifi = self.visit_df['ns_radio'].str.contains("wifi").sum()
        wwan = self.visit_df['ns_radio'].str.contains("wwan").sum()
        if (wwan + wifi) > 0.0:
            single_visit_df["mobility_rate"][0]= float(float(wwan) / (float(wwan) + float(wifi)))
        else:
            single_visit_df["mobility_rate"][0] = -1

        # average_session_duration
        single_visit_df["avg_session_duration"][0]= self.visit_df["Foreground_time"].mean()

        # sd_session_duration
        single_visit_df["sd_session_duration"][0]= self.visit_df['Foreground_time'].std(ddof=0)

        # longest_inactivity
        last= self.visit_df["ns_utc"].iloc[0]
        max_inact = dt.timedelta(hours=0)
        last_visit = 0
        for i in range(1, len(self.visit_df)):
            if self.visit_df["ns_utc"].iloc[i] - last > max_inact:
                max_inact = self.visit_df["ns_utc"].iloc[i] - last
                single_visit_df["last_visit_before_li"].iloc[0] = last
                single_visit_df["number_visits_after_li"].iloc[0] = len(self.visit_df)-i
            last = self.visit_df["ns_utc"].iloc[i]
        single_visit_df["longest_inactivity"].iloc[0]= max_inact.total_seconds()

        # last_visit
        single_visit_df["last_visit"].iloc[0] = self.visit_df["ns_utc"].iloc[len(self.visit_df)-1]

        # churn
        binary_churn, binary_inactivity_60_d, binary_inactivity_90_d, churn_30d, churn_60d, churn_90d, churn_time= 0, 0, 0, 0, 0, 0, 0
        last_before_churn_time = self.churn_binary(self.visit_df, self.analysis_start, self.analysis_end, max_days=0, churn_delay=0)
        #print("new last before churn: ", last_before_churn_time)
        # test longer churn times when no churn was found
        if last_before_churn_time != False:
            binary_churn = True
            churn_time = last_before_churn_time + dt.timedelta(days=30)
            churn_time_30d = self.churn_binary(self.visit_df, self.analysis_start, self.analysis_end,max_days=37)
            if churn_time_30d != False:
                churn_30d, churn_60d, churn_90d = 1, 1, 1
            else:
                churn_time_60d = self.churn_binary(self.visit_df, self.analysis_start,self.analysis_end, max_days=67)
                if churn_time_60d != False:
                    churn_60d, churn_90d = 1, 1
                else:
                    churn_time_90d = self.churn_binary(self.visit_df, self.analysis_start, self.analysis_end, max_days=97)
                    if churn_time_90d != False:
                        churn_90d = 1

        # 60 und 90 Tage Churn berechnen
        last_before_churn_time = self.churn_binary(self.visit_df, self.analysis_start, self.analysis_end, max_days=0, churn_delay=0, churn_days=60)
        if last_before_churn_time != False:
            binary_inactivity_60_d =True

        last_before_churn_time = self.churn_binary(self.visit_df, self.analysis_start, self.analysis_end,
                                                   max_days=0, churn_delay=0, churn_days=90)
        if last_before_churn_time != False:
            binary_inactivity_90_d = True
        #print("churn, churn_30d, churn_60d, churn_90d:", binary_churn, churn_30d, churn_60d, churn_90d)
        single_visit_df["binary_churn"][0] = binary_churn
        single_visit_df["last_before_churn_time"][0] = last_before_churn_time
        single_visit_df["churn_time"][0] = churn_time
        single_visit_df["churn_30d"][0]= churn_30d
        single_visit_df["churn_60d"][0]= churn_60d
        single_visit_df["churn_90d"][0]= churn_90d
        single_visit_df["binary_inactivity_60_d"][0] = binary_inactivity_60_d
        single_visit_df["binary_inactivity_90_d"][0] = binary_inactivity_90_d

        # Average session duration by time of day
        #TODO Zeitoptimieren
        for x in range(24):
            value_list = []
            for i in range(len(self.visit_df)):
                if self.visit_df['ns_utc'][i].time().hour==x:
                    value_list.append(self.visit_df["Foreground_time"].iloc[i])
            if len(value_list)>0:
                single_visit_df["visit_h_" + str(x)][0]= len(value_list)
                single_visit_df["sd_visit_duration_h_" + str(x)][0]= np.std(value_list)
                single_visit_df["avg_visit_duration_h_" + str(x)][0]= np.mean(value_list)
            else:
                single_visit_df["visit_h_" + str(x)][0]= 0
                single_visit_df["avg_visit_duration_h_" + str(x)][0]= 0
                single_visit_df["sd_visit_duration_h_" + str(x)][0]= 0

        # Anzahl Visits mit einer bestinmmten Featurenutzung berechnen
        # visits_Übersicht usw
        for f in self.feature_list:
            count = 0
            for i in range(len(self.visit_df)):
                if self.visit_df[f].iloc[i]>0:
                    count += 1
            single_visit_df["visits_" + f][0]= count

        # Anzahl Visits in den verschiedenen Zeiträumen nach Inastallation berechnen
        # visits_24_h usw
        # visits_dauer_24_h
        """ version kusi 
        delta_min = dt.timedelta(hours=0)
        for t in self.timerange_list:
            delta_max = dt.timedelta(hours=t)
            count = 0
            dur = 0.0
            for i in range(len(self.visit_df)):
                period_start = self.visit_df["ns_ap_gs"][0] + delta_min
                period_end = self.visit_df["ns_ap_gs"][i] + delta_max
                if self.visit_df["ns_ap_gs"][0] + delta_min < self.visit_df["ns_utc"][i] and self.visit_df["ns_ap_gs"][i] + delta_max > self.visit_df["ns_utc"][i]:
                    diff = self.visit_df["ns_utc"][i] - self.visit_df["ns_ap_gs"][0]
                    print "counting up kusi: ", self.visit_df["ns_ap_gs"][0], " - ", self.visit_df["ns_utc"][i], "difference hours: ", diff
                    count += 1
                    dur += float(self.visit_df["Foreground_time"][i])
            single_visit_df["visits_" + str(t) + "_h"][0]= count
            single_visit_df["visits_dauer_" + str(t) + "_h"][0]= dur
            print "kusi_visits_", str(t), "_h:", count, ", ", "dauer_", str(t), "_h:", dur
            delta_min = delta_max """
        #----------------------------------------------------------------------
        rangeindex = 0
        ccount = 0
        cdur = 0.0
        period_max = self.visit_df["ns_ap_gs"].iloc[0] + dt.timedelta(hours=336) #max for *zeitraum* - Variables
        for i in range(len(self.visit_df)):
            diff = self.visit_df["ns_utc"].iloc[i] - self.visit_df["ns_ap_gs"].iloc[0]
            diff = diff.total_seconds()
            h24 =dt.timedelta(hours=24).total_seconds()
            current_range_index = int(diff/ h24)
            #----- move to end ----------------
            ccount += 1
            cdur += float(self.visit_df["Foreground_time"].iloc[i])
            if i == 0:
                cstd = 0.0
            else:
                temp_df = self.visit_df['Foreground_time'].iloc[:i+1]
                cstd = temp_df.std(ddof=0)
            #print "checking krs: ", self.visit_df["ns_ap_gs"][0], " - ", self.visit_df["ns_utc"][i], "difference hours: ", diff
            period_end = self.visit_df["ns_utc"].iloc[i] + dt.timedelta(hours=self.timerange_list[rangeindex]) #

            # if the visit is outside the max time period, the last values are being stored
            if self.visit_df["ns_utc"].iloc[i] > period_max:
                #single_visit_df["visits_" + str(self.timerange_list[rangeindex]) + "_h"].iloc[0] = ccount # overestimating, as the current visit outside the timerange is being counted
                #single_visit_df["visits_dauer_" + str(self.timerange_list[rangeindex]) + "_h"][0]= cdur # overestimating, as the current visit outside the timerange is being counted
                #single_visit_df["visits_regel_" + str(self.timerange_list[rangeindex])][0] = cstd # overestimating, as the current visit outside the timerange is being counted
                break


            # Set the current Zeitraum and all longer Zeiträume to the current values
            # If another Visit wihtin this timerange is being found, the values will be overwritten
            for j in range(current_range_index, len(self.timerange_list)):
                single_visit_df["visits_" + str(self.timerange_list[j]) + "_h"][0] = ccount
                single_visit_df["visits_dauer_" + str(self.timerange_list[j]) + "_h"][0] = cdur
                single_visit_df["visits_regel_" + str(self.timerange_list[j])][0] = cstd
                for feature in self.feature_list:
                    if self.visit_df[feature].iloc[i] > 0:
                        single_visit_df[feature + "_" + str(self.timerange_list[j]) + "_h"][0] += 1


            # Loop through all *zeiträume*, until you find one that's within the specified visit!!
            # If the visits is after the *zeitraum* period (e.g. 24 - 48 hours), then set the period to Zero and check if
            # the visit is within the next period (rangeindex += 1).
            """"
            while self.visit_df["ns_utc"][i] > period_end and self.visit_df["ns_utc"][i] < period_max and rangeindex < len(self.timerange_list):
                single_visit_df["visits_" + str(self.timerange_list[rangeindex]) + "_h"][0] = ccount
                single_visit_df["visits_dauer_" + str(self.timerange_list[rangeindex]) + "_h"][0]= cdur
                single_visit_df["visits_regel_" + str(self.timerange_list[rangeindex])][0] = cstd
                #print "krs_visits_", str(self.timerange_list[rangeindex]), "_h:", ccount, ", ", "dauer_", str(self.timerange_list[rangeindex]), "_h:", cdur

        #-- neu in diesem loop.
                for feature in self.feature_list:
                    if self.visit_df[feature].iloc[i] > 0:
                        single_visit_df[feature + "_" + str(self.timerange_list[rangeindex]) + "_h"][0] += 1
                        # print "krs ->", feature+"_"+str(self.timerange_list[rangeindex])+"_h = ",single_visit_df[feature+"_"+str(self.timerange_list[rangeindex])+"_h"][0]
        #-------------------------------------

                rangeindex += 1
                if rangeindex < len(self.timerange_list): #if a next period_end exists...                 
                    period_end = self.visit_df["ns_utc"].iloc[i] + dt.timedelta(hours=self.timerange_list[rangeindex])
            #print "counting up krs"
            """

            
                    
                    
        """ ------------ removed, old by kusi
        # Anzahl Visits eines bestimmten Features in einem bestimmten Zeitraum nach download
        # Übersicht_24_h usw
        delta_min = dt.timedelta(hours=0)
        for f in self.feature_list:
            delta_min = dt.timedelta(hours=0)
            for t in self.timerange_list:
                delta_max = dt.timedelta(hours=t)
                count = 0
                for i in range(len(self.visit_df)):
                    start = self.visit_df["ns_ap_gs"][0] + delta_min
                    if self.visit_df["ns_ap_gs"][0] + delta_min < self.visit_df["ns_utc"][i] and self.visit_df["ns_ap_gs"][i] + delta_max > self.visit_df["ns_utc"][i] and self.visit_df[f][i]>0:
                        count += 1
                        print "kusi ->", f+"_" + str(t) + "_h=", count
                single_visit_df[f+"_" + str(t) + "_h"][0]= count
                delta_min = delta_max
                ---------------------"""
                
        self.collapsetimelogger.info("run %s; collapsing; %s; %s",self.aggregator_start_time, len(self.visit_df), time.strftime('%H:%M:%S', time.gmtime(time.time()-start_time)))
        return single_visit_df