#######################################################################
## Replicative R Code                                                ##
## Data Science                                                      ##
## david.schwelien@srgssr.ch, Bern April 2016                        ##
#######################################################################
require(plyr)
require(lubridate)
# project folder:
# # SharePoint/Data Science - Documents/
#######################################################################
##  0. Datareading                                                   ##
#######################################################################
rm(list=ls()) #will remove ALL objects 
#raw=  read.table("C:/Users/schwelda/SharePoint/Data Science - Documents/Datenbeschaffung/Sample4/srg_srf-meteo-android+srf-meteo-ios_20150101-20150331_20464 (1).tsv"               , sep="\t")
# dta =  read.csv("C:/Users/schwelda/SharePoint/Data Science - Documents/Datenbeschaffung/Sample4_collapsed/Event_Collapsed_1463652938.csv"  , row.names=1, sep=";")
#dta =  read.csv("C:/Users/schwelda/SharePoint/Data Science - Documents/Datenbeschaffung/Sample4_collapsed/Event_Collapsed_1463666015.csv" , row.names=1, sep=";")
#dta =  read.csv("E:/Datenbeschaffung/Sample4_merged/collapsed/Event_Collapsed_2016-07-29 1528.csv"
#                , row.names=1, sep=";")
dta =  read.csv("E:/Datenbeschaffung/Sample4_redef_start_unrestriced/collapsed/Event_Collapsed_2016-09-16 0732.csv"
                , row.names=1, sep=";")
#dta_1 =  read.csv("E:/Datenbeschaffung/Event_Collapsed_1464254588.csv", row.names=1, sep=";")
#dta_2 =  read.csv("E:/Datenbeschaffung/Event_Collapsed_1464254588_2.csv", row.names=1, sep=";")

# merge files
#dta= rbind(dta_1, dta_2) 
#View(dta)
#######################################################################
##  1. Rescaling / gernerating variables                             ##
#######################################################################
names(Filter(is.factor, dta)) # identify vars  with scaling other than numierc (numeric=Integer)
#head(dta[,names(Filter(is.factor, dta))])

#store browser id as variable
dta$browser_id <- rownames(dta)
unique(dta$browser_id == rownames(dta))

####      ns_ap_gs
dta$ns_ap_gs = as.POSIXct(dta$ns_ap_gs, format="%Y-%m-%d %H:%M:%S") #declare time format
dta$datum <- as.Date(dta$ns_ap_gs)
####      Device
levels(dta$Device) #alles OK

####      Manufacturer
levels(dta$Manufacturer) # alles OK 

library(lubridate)
####      last_before_churn_time
dta$last_before_churn_time[which(dta$last_before_churn_time=="False")]=NA # korrect for NAs
dta$last_before_churn_time = as.POSIXct(dta$last_before_churn_time, format="%Y-%m-%d %H:%M:%S") #declare time format

####      churn_time
dta$churn_time[which(dta$churn_time==0)]=NA # korrect for NAs
dta$churn_time = as.POSIXct(dta$churn_time, format="%Y-%m-%d %H:%M:%S") #declare time format

####      last_visit_before_li
dta$last_visit_before_li = as.POSIXct(dta$last_visit_before_li, format="%Y-%m-%d %H:%M:%S") #declare time format

####      last_visit
dta$last_visit = as.POSIXct(dta$last_visit, format="%Y-%m-%d %H:%M:%S") #declare time format

#### 1.2 Vars than need to be rescaled to as.duration:
####      longest_inactivity
dta$longest_inactivity = as.duration(dta$longest_inactivity) # trensfer to duration class (lubridate)
dta$longest_inactivity[which(dta$longest_inactivity==0)]=NA # correct for duration = 0 ; as this was generated for single visits




#### 1.5 Vars than need to be rescaled to Factor:
library(plyr)
####      Platform
dta$Platform = revalue(as.factor(dta$Platform) , c("0"="mobile", "1"="tablet", "2"="unknown"))
####      Operating.system
dta$Operating.system = revalue(as.factor(dta$Operating.system) , c("0"="Android", "1"="IOS", "2"="unknown"))
####      Country
dta$Country = revalue(as.factor(dta$Country) , c("1"="nicht Schweiz"))
####      installation_time
summary(as.factor(dta$installation_time))
dta$installation_time = revalue(as.factor(dta$installation_time) 
                                , c("0"="22-06 Uhr", "1"= "06-12 Uhr","2"="12-14 Uhr","3"="14-18 Uhr","4"="18-22 Uhr"))
####      installation_weekend
dta$installation_weekend = revalue(as.factor(dta$installation_weekend) 
                                , c("0"="installed SA or SO", "1"="installed OTHER THAN SA or SO"))
####      installation_season
#basil bug:
dta$installation_season <- as.factor(dta$installation_season)
dta$installation_season <- NA
levels(dta$installation_season) <- c("spring", "winter", "summer", "autumn")
head(dta$installation_season)

frühlingsanfang.14 <- as.POSIXct("2014-03-21 00:00:01") 
frühlingsanfang.15 <- as.POSIXct("2015-03-21 00:00:01") 
frühlingsanfang.16 <- as.POSIXct("2016-03-21 00:00:01")
frühlingsanfang.17 <- as.POSIXct("2017-03-21 00:00:01") 


sommeranfang.14 <- as.POSIXct("2014-06-21 00:00:01")
sommeranfang.15 <- as.POSIXct("2015-06-21 00:00:01")
sommeranfang.16 <- as.POSIXct("2016-06-21 00:00:01")

herbstanfang.14 <- as.POSIXct("2014-09-23 00:00:01")
herbstanfang.15 <- as.POSIXct("2015-09-23 00:00:01")
herbstanfang.16 <- as.POSIXct("2016-09-23 00:00:01")

winteranfang.14 <- as.POSIXct("2014-12-22 00:00:01")
winteranfang.15 <- as.POSIXct("2015-12-22 00:00:01")
winteranfang.16 <- as.POSIXct("2016-12-22 00:00:01")

frühling <- c(frühlingsanfang.14 : sommeranfang.14, 
              frühlingsanfang.15 : sommeranfang.15, 
              frühlingsanfang.16 : sommeranfang.16)

sommer <- c(sommeranfang.14 : herbstanfang.14, 
            sommeranfang.15 : herbstanfang.15, 
            sommeranfang.16 : herbstanfang.16)


herbst <- c(herbstanfang.14 : winteranfang.14, 
            herbstanfang.15 : winteranfang.15, 
            herbstanfang.16 : winteranfang.16)


winter <- c(winteranfang.14 : frühlingsanfang.15, 
            winteranfang.15 : frühlingsanfang.16,
            winteranfang.16 : frühlingsanfang.17)


dta[dta$ns_ap_gs %in% frühling, "installation_season"] <- "spring" 
dta[dta$ns_ap_gs %in% sommer, "installation_season"] <- "summer" 
dta[dta$ns_ap_gs %in% herbst, "installation_season"] <- "autumn" 
dta[dta$ns_ap_gs %in% winter, "installation_season"] <- "winter" 
summary(is.na(dta$ns_ap_gs))
dta$installation_season <- as.factor(dta$installation_season)
head(dta[, c("ns_ap_gs", "installation_season")])

#plot(dta$ns_ap_gs, dta$installation_season)

#### 1.6 Numeric vars that need to be corrected: 
dta$mobility_rate[dta$mobility_rate<0] = NA

#### 1.7 Vars than need to be rescaled to as.logical:
dta$binary_churn = dta$binary_churn==1
#dta$churn_30d = dta$churn_30d==1
#dta$churn_60d = dta$churn_60d==1
#dta$churn_90d = dta$churn_90d==1
#dta$ns_radio = dta$ns_radio==1

#######################################################################
##  2.1. visiting data by time after installation                    ##
#######################################################################

vars =  grep("visits_", names(dta), value=TRUE) ### get all the variables that contain the number of visits after time x
sort(vars)[1:14] # check whether you got all of them beolw! 
v_005_day_visits = dta$visits_120_h
v_006_day_visits = dta$visits_144_h
v_007_day_visits = dta$visits_168_h
v_008_day_visits = dta$visits_192_h
v_009_day_visits = dta$visits_216_h
v_001_day_visits = dta$visits_24_h
v_010_day_visits = dta$visits_240_h
v_011_day_visits = dta$visits_264_h
v_012_day_visits = dta$visits_288_h
v_013_day_visits = dta$visits_312_h
v_014_day_visits = dta$visits_336_h
v_002_day_visits = dta$visits_48_h
v_003_day_visits = dta$visits_72_h
v_004_day_visits = dta$visits_96_h

dta_day_visits = data.frame(
  v_005_day_visits ,
  v_006_day_visits ,
  v_007_day_visits ,
  v_008_day_visits ,
  v_009_day_visits ,
  v_001_day_visits ,
  v_010_day_visits ,
  v_011_day_visits ,
  v_012_day_visits ,
  v_013_day_visits ,
  v_014_day_visits ,
  v_002_day_visits ,
  v_003_day_visits ,
  v_004_day_visits )
dta_day_visits = dta_day_visits[, order(names(dta_day_visits))] ### combine these variables to new dataframe 

dta_day_visits_new = dta_day_visits #### make them not accumolated
for(i in 2:ncol(dta_day_visits)){
  dta_day_visits_new[,i] = dta_day_visits[,i] - dta_day_visits[,i-1]
}
dta = cbind(dta, dta_day_visits_new )### bind to data

#######################################################################
##  2.2. device.data and wetter.data                   ##
#######################################################################

device.data =  read.csv("data/device.data.csv"
                        , sep=";")
device.data$release_date <- as.POSIXct(device.data$release_date, format="%B %d, %Y") #declare time format
dta <- merge(dta, device.data,  by = "Device")


wetter <- read.csv("E:/datenauswertung/2015_05_31_Data_Science/data/wetter.csv", sep=";")
wetter$datum <- as.numeric(paste(wetter$datum))
wetter$datum <-  as.Date(wetter$datum, origin = "1899-12-30")
wetter[,2] <- as.numeric(paste(wetter[,2]))
wetter[,3] <- as.numeric(paste(wetter[,3]))
wetter[,4] <- as.numeric(paste(wetter[,4]))
#wetter <- na.omit(wetter)
#wetter$z.temp <- - scale(wetter$temp)
#wetter$z.sonnenscheindauer <- scale(wetter$sonnenscheindauer)
#wetter$z.niederschlag <- scale(wetter$niederschlag)
#plot(wetter$datum, wetter$temp)
wetter <- wetter[,1:2]
wetter <- na.omit(wetter)
dta <- merge(dta, wetter,  by = "datum", all.x = T)

dta$datum <- NULL # info is in ns_ap_gs

sort(names(dta))
#######################################################################
##  2.2. alternative churn definition                                ##
#######################################################################
#dta$churn = (dta$longest_inactivity >= 90*60*60*24 | dta$Visits  == 1)
#cut.off.date = as.POSIXct("2015-08-31 23:59:59 CET")
#dta$churn[dta$ns_ap_gs>=cut.off.date] = NA
summary(dta$churn_time)
dta <- subset(dta, Visits > 1) # data has to be subset; as many vars will be meaningless if Visits =1
Encoding(names(dta)) <- "UTF-8"

#######################################################################
##  2.3. rescale time vars (to calculate linear predictor)           ##
#######################################################################

names(Filter(is.POSIXct, dta))
dta$ns_ap_gs.z <- as.numeric(dta$ns_ap_gs - min(dta$ns_ap_gs, na.rm = T), units = "days")
dta$last_before_churn_time.z <- as.numeric(dta$last_before_churn_time - min(dta$last_before_churn_time, na.rm = T), units = "days")
dta$churn_time.z <- as.numeric(dta$churn_time - min(dta$churn_time, na.rm = T), units = "days")
dta$last_visit_before_li.z <- as.numeric(dta$last_visit_before_li - min(dta$last_visit_before_li, na.rm = T), units = "days")
dta$last_visit.z <- as.numeric(dta$last_visit - min(dta$last_visit, na.rm = T), units = "days")
dta$release_date.z <- as.numeric(dta$release_date - min(dta$release_date, na.rm = T), units = "days")

#######################################################################
##  2.1. visiting data by time after installation                    ##
#######################################################################
save(dta, file="data/dta_3.0.RData")
#save(raw, file="data/raw.RData")
rm(list=ls()) #will remove ALL objects 

#### reste 
#markus_data = as.data.frame(sort(table((dta$Device))))

#View(markus_data)
#write.csv(markus_data, file="data/for.markus.csv")
#### device data input 


#plot(wetter$datum, wetter$temp , type="l" )
#plot(wetter$datum, wetter$niederschlag)
#lines(smooth.spline(wetter$datum, wetter$niederschlag, df = 10), lty = 2, col = "red")