file = "E:/Datenbeschaffung/Sample4_redef_start_unrestriced/collapsed/Event_Collapsed_2016-09-16 0732.csv" # please note: "/" is correct; \ will cause error

if (!exists("dta")) {
  dta =  read.csv(file
                  , row.names=1, sep=";")
  }    


# NOTE: for Linux/IOS source packages installation, GFortran, BLAS, LAPACK libraries need to be installed in the system
# e.g.: sudo apt-get install libblas-dev liblapack-dev gfortran
# devtools::install_version("cluster", version = "2.0.3", repos = "http://cran.rstudio.com")
# devtools::install_version("NMF", version = "0.20.6", repos = "http://cran.rstudio.com")
# devtools::install_version("fastICA", version = "1.2-0", repos = "http://cran.rstudio.com")
# devtools::install_version("Ckmeans.1d.dp", version = "3.3.1", repos = "http://cran.rstudio.com")

    
base_packages = c("hash","plyr","dplyr","data.table","reshape2","ggplot2","Matrix","lubridate")
stat_packages = c("randomForest","xgboost","mclust","Ckmeans.1d.dp","fastICA","NMF")   # "dynamicTreeCut"   # "rpart   # "C50"


# WARNING: combine from randomForest should not be loaded after combine from foreach package, since it will break parallel computations
packages = c(base_packages, stat_packages)


for (package in packages) {
    if (!require(package, character.only = TRUE)) {
        install.packages(package, repos="http://cran.rstudio.com/")
        library(package, character.only = TRUE)
    }
}
