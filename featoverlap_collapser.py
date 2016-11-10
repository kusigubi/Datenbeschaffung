# -*- coding: utf-8 -*-
"""
Created on Sat Apr 23 11:42:46 2016
# Collapsing Objekt für ein DataFrame mit Browsers
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

Functions
---------


@author: gublermr, burgerch
"""
import pandas as pd
import numpy as np
import time
import datetime as dt
import logging

class Featoverlap_collapser(object):
    def __init__(self, visit_df, aggregator_start_time, analysis_start, analysis_end):
        self.aggregator_start_time = aggregator_start_time  # for logging reasons
        self.log = logging.getLogger('standard_logger')
        self.collapsetimelogger = logging.getLogger('time_logger')
        # print("New Collapser for ", visit_df["Browsers"][0], ";" , len(visit_df))
        if visit_df.size == 0:
            raise ValueError('Visit DF for Browser : ' + visit_df["Browsers"][0] + " was empty!")

        self.analysis_end = analysis_end
        self.analysis_start = analysis_start
        self.visit_df = visit_df
        self.installation_timestamp = self.visit_df['ns_ap_gs'][0]
        self.feature_list = ["Übersicht", "Meine Favoriten", "Such Resultate", "Radar", "Prognose Schweiz", "Schnee",
                             "Wetterbericht", "Meteo News", "Über uns", "Warnungen Warncenter", "Impressum",
                             "Widget Add", "Widget click", "App install", "App open", "Landingpage", "Karte", "Artikel",
                             "Add Favorite"]

        self.returnvalues_default = self.define_defaults()
        # self.sort_visits()
        self.format_visits()
        print "---------------------------------New feature collapser for:", visit_df["Browsers"][0]

    def define_defaults(self):
        # Default Werte für alle Spalten. Muss gegebenenfalls als Zahl definiert werden. Bis jetzt alles einfach String...
        defaultvalues = {"Browsers": [""], "Visits": [""], "ns_ap_gs": [""]}

        # Automatisch generierte Variablen anhängen
        # Features and Cross-Features

        feature_list_copy = list(self.feature_list)
        cross_list = []

        for feat in self.feature_list:
            feature_list_copy.pop(0)
            for feat2 in feature_list_copy:
                defaultvalues["Feat_" + feat + "_" + feat2] = 0

        return defaultvalues

    """
    Copy ns_ap_gs (installation time) to all visit rows (not just first one)
    Transform timestamps (default in miliseconds from comScore) to second-values (default in py-datetime)
    """
    def format_visits(self):
        self.visit_df = self.visit_df.convert_objects(convert_numeric=True)
        # print visit_df["ns_ap_gs"]
        for i in range(len(self.visit_df)):
            self.visit_df['ns_ap_gs'][i] = self.visit_df['ns_ap_gs'][0]
        self.visit_df['ns_ap_gs'] = [dt.datetime.fromtimestamp(t / 1000) for t in self.visit_df.ns_ap_gs]
        self.visit_df['ns_utc'] = [dt.datetime.fromtimestamp(t / 1000) for t in self.visit_df.ns_utc]


    def columns_order(self):
        # Die hier definierte Reihenfolge wird in den Output geschrieben. Wenn oben eine neue Spalte definiert wird, muss sie auch für den export definiert werden
        colums_order = ["Browsers", "Visits", "ns_ap_gs"]
        feature_list_copy = list(self.feature_list)

        for feat in self.feature_list:
            feature_list_copy.pop(0)
            for feat2 in feature_list_copy:
                colums_order.append("Feat_" + feat + "_" + feat2)

        #print "columns_order:", colums_order
        return colums_order

    """ Berechnen der Häufigkeiten der überlappenden Featurenutzung"""
    def cross_features_calc(self):
        start_time = time.time()
        single_visit_df = pd.DataFrame(self.returnvalues_default)

        """Bringt nichts (2 Minuten/Stunde)
        #kill all the columns that weren't used
        for x, feat in enumerate(self.feature_list):
            if self.visit_df[feat].sum() == 0:
                self.feature_list.pop(x)
        """

        feature_list_copy = list(self.feature_list)

        for feat in self.feature_list:
            feature_list_copy.pop(0)
            for i in range(len(self.visit_df)):
                if self.visit_df[feat].iloc[i]>0:
                    for feat2 in feature_list_copy:
                        count = 0
                        if self.visit_df[feat2].iloc[i]>0:
                            #print "found usage on: ", feat, " and on ", feat2
                            single_visit_df["Feat_" + feat + "_" + feat2][0] += 1
                            #print "Count is:", count, "adding to: ", "Feat_" + feat + "_" + feat2 + ":", single_visit_df["Feat_" + feat + "_" + feat2][0]

        single_visit_df["Browsers"][0] = self.visit_df["Browsers"][0]
        single_visit_df["Visits"][0] = len(self.visit_df)
        single_visit_df["ns_ap_gs"][0] = self.visit_df["ns_ap_gs"][0]

        return single_visit_df