#' Load Relevant Libaries
#'
#' Loads some commonly used libraries.
#' If library does not exist, it will be installed.
#' @param
#' @keywords library, libaries
#' @export
#' @examples
#' loadLibraries()
loadLibraries <- function(){
  #Define data frame containing commonly used packages.
  pkgs <- data.frame(name=c("RODBC", "getPass", "sqldf", "RSQLite", "lubridate",
                            "ggplot2", "ggrepel", "scales", "reshape2",
                            "dplyr","magrittr", "readr", "readxl", "tibble", "tidyr",
                            "psych", "pROC", "pander"),
                     details=c("RODBC - Database connections",
                               "getPass - secure password input without RStudio dependency",
                               "sqldf - Execute SQL code on data frames",
                               "RSQLite - SQLite interface",
                               "lubridate - Makes working with dates easily",
                               "ggplot2 - Grammar of Graphics plotting",
                               "ggrepel - geom_text_repel and geom_label_repel for non-overlapping text and labels",
                               "scales - For color alpha",
                               "reshape2 - For pivot style tables",
                               "dplyr - To more easily manipulate data frames",
                               "magrittr - Forward pipe operator %>%, exposition operator %$%, and compound assignment pipe operator %<>%",
                               "readr - Import data from flat files (csv, tsv, fwf, ...)",
                               "readxl - Import data from Excel workbooks",
                               "tibble - An improved implementation of data.frame",
                               "tidyr - Makes it easy to tidy your data",
                               "psych - contains pairs.panels() function",
                               "pROC - ROC plot and AUC",
                               "pander - Format tables for Word documents."),
                     stringsAsFactors=FALSE
  )
  # Loop through packages. If they are installed, load them.
  # If they are not installed, install them, then load them.
  print_str <- "Packages Loaded:"
  for(i in 1:NROW(pkgs)){
    #i<-5
    if(require(pkgs$name[i], character.only=TRUE)){
      #library(pkgs$name[i], character.only=TRUE, verbose=TRUE)
      print_str <- paste(print_str, paste("  ", pkgs$details[i]), sep = "\n")
    } else {
      install.packages(pkgs$name[i])
      library(pkgs$name[i], character.only=TRUE)
      print_str <- paste(print_str, paste("Installed - ", pkgs$details[i]), sep = "\n")
    }
  }
  # Print details of loading.
  cat(print_str)
}