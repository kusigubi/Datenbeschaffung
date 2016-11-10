#######################################################################
## Replicative R Code ##
## glm improving algorithm ##
## david.schwelien@srgssr.ch, Bern April 2016 ##
#######################################################################
rm(list=ls())
#will remove ALL objects 
load("E:/datenauswertung/2015_05_31_Data_Science/data/dta.RData")
#to replicate: dataset on request
dta <- subset(dta, Visits > 1) # data has to be subset; as many vars will be meaningless if Visits =1
Encoding(names(dta)) <- "UTF-8"
dta$Device<-NULL
#### loop over variables
loop = names(dta)
endogenous_vars <- c("last_visit","binary_churn", "last_before_churn_time", "churn_time" 
 , "sd_session_duration" , "longest_inactivity", "last_before_churn_time.z" , "last_visit.z", "churn_time.z") # take out vars that cause endogeneity
loop = loop[!loop %in% endogenous_vars]
bestvars <- vector()

rob_time <- c("Visits","number_visits_after_li","visit_morgen","visit_vormittag","visit_mittag","visit_nachmittag"
,"visit_abend","avg_visit_duration_morgen","avg_visit_duration_vormittag","avg_visit_duration_mittag"
,"avg_visit_duration_nachmittag","avg_visit_duration_abend","sd_visit_duration_morgen","sd_visit_duration_vormittag"
,"sd_visit_duration_mittag","sd_visit_duration_nachmittag","sd_visit_duration_abend","visits_?bersicht"
,"visits_Meine.Favoriten","visits_Such.Resultate","visits_Radar","visits_Prognose.Schweiz","visits_Schnee"
,"visits_Wetterbericht","visits_Meteo.News","visits_?ber.uns","visits_Warnungen.Warncenter","visits_Impressum"
,"visits_Widget.Add","visits_Widget.click","visits_App.install","visits_App.open","visits_Landingpage","visits_Karte"
,"visits_Artikel","visits_Add.Favorite") 

dates <- names(Filter(is.POSIXct, dta))

loop <- loop[!loop %in% rob_time]
loop <- loop[!loop %in% dates]

### the inital 'champion' is just the empty model
func = "binary_churn ~ 1"
model_A = glm(func, data = dta ,family=binomial(link='logit')) # replace the model
benchmark = Inf
round_count=0

while(model_A$aic <= benchmark && round_count<30) {### run until model can not improve
   # emptry the workspace, or R will die
  file = paste0("E:/datenauswertung/2015_05_31_Data_Science/data/_second_run_after_rob_time_after_device_merge_algorithm_results_round_",round_count,".RData", sep = "")
  save(list=ls(),  file=file)
  load(file)
  # at the beginning of each round set benchmark and round_count
  benchmark = model_A$aic
  round_count = round_count + 1
  # generate dataframe to store regression results
  now_model_aics <- data.frame(func = rep(NA, length(loop))
                               , looped_var = rep(NA, length(loop))
                               , aic = rep(NA, length(loop)))
  class(now_model_aics$aic) <- "numeric"
  # then loop to set up the compeating models
  for(i in 1:length(loop)){ #loop over all independent variables
    func <- paste0(bestvars, sep=" + ", collapse = "") # write the funtion of the glm() using the bestvars START .... 
    func <- substring(func,1,nchar(func)-2) # (necessary, because '+ ' symbol at end)
    func <- paste("binary_churn ~" , func , " + ",  loop[i]) #... END
    now_model <- glm(func, data = dta ,family=binomial(link='logit')) # replace the model
    now_model_aics[i, ] <-  c(func, loop[i],  as.numeric(extractAIC(now_model)[2]))
  }
  now_model_aics <- now_model_aics[order(now_model_aics$aic),]
  bestvars[[length(bestvars)+1]] = now_model_aics[1, "looped_var"] # this list will contain all bestvars per round
  loop <- loop[!loop %in% bestvars]
  func <- now_model_aics[1, "func"]
  model_A = glm(func, data = dta ,family=binomial(link='logit')) # replace the model
}

summary(model_A)
