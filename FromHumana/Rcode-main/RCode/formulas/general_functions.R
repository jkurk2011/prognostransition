# File: General Functions
# Author: Jake Gaecke
# Description: This file contains custom functions that have been written to simplify common tasks.


#### General Purpose ####

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




#' Copy To Clipboard
#'
#' Copies a data frame to the clipboard in a format that is pasteable as a table (for example, in Excel)
#' @param dat Data Frame to be copied
#' @param col_names Defaults to
#' @keywords copy, data frame, export
#' @export
#' @examples
#' copyToClipboard()
copyToClipboard <- function(dat, col_names=TRUE, row_names=FALSE){
  # Writes dat to the clipboard
  # dat = data frame to by copies
  # row_names = copy row names?
  write.table(dat, "clipboard", sep="\t", col.names=col_names, row.names=row_names)
  dat # pass-through data in case you want to put this in the middle of a larger calculation
}

#' Read Clipboard
#'
#' Reads data from the clipboard
#' @param
#' @keywords copy, data frame, import
#' @export
#' @examples
#' dat <- readClipboard()
readClipboard <- function(){
  # reads data from the clipboard
  return(read.delim("clipboard"))
}


#' Fix Dates
#'
#' Converts all date-time values to Dates to remove useless time values.
#' @param dat Data Frame to be modified.
#' @keywords date, data frame, POSIXct, datetime, instant
#' @export
#' @examples
#' fixDates(dat)
fixDates <- function(dat) {
  # Dates often imported as Date-Time values in one of various formats.
  # To avoid some of the complications of working with dates, this function
  # converts Date or POSIXct dates to Dates with no time component.
  # This function returns the same data frame passed in with Date or POSIXct dates converted to Date.
  library(lubridate)
  dat <- as.data.frame(dat) # tbl_df for some reason causes problems with is.instant()
  for(j in 1:NCOL(dat)){
    if (is.instant(dat[,j])) {
      dat[ ,j] <- as.Date(dat[ ,j], tz = "UTC")
    }
  }
  return(dat)
}

#' Case
#'
#' Provides case statement functionality inside of mutate function from dplyr.
#' @param x Column that has multiple values that you want to reassign to new values.
#' You must also provide current values = new values, delimited by commas. See example.
#' @keywords case, if
#' @export
#' @examples
#' mutate(dat,
#'        x = case(column,
#'                 "A" = 1,  #left side has values that column might take
#'                 "B" = 2)) #right side has the value you want to replace with
case <- function(x, ...){
  #Example:
  # mutate(x = case(column,
  #                  "A" = 1,  #left side has values that column might take
  #                  "B" = 2)) #right side has the value you want to replace with
  if(is.factor(x)){
    x <- as.character(x)
  }
  return(sapply(x, switch, ...))
}


#### SQL Functions ####

#' Read SQL
#'
#' Reads a *.SQL file into a vector of strings.
#' SQL commands are automatically split by ":". SQL commands starting with "--" are not removed, since scripts sometimes starts with a comment. However, SQL commands between /* SQL CODE */ are removed. NULL SQL commands are also removed.
#' @param loc exact location of a *.SQL file.
#' @keywords sql, import, read
#' @export
#' @examples
#' readSQL(strLocation)
readSQL <- function(loc) {
  # Reads a .SQL file into a vector of strings
  # loc = exact location of .SQL file
  # SQL commands are automatically split by ";"
  # SQL commands starting with "--" are  not removed, since sometimes scripts start with a comment
  # SQL commands between /* SQL COMMAND HERE */ are removed
  # NULL SQL commands are removed

  # Import raw SQL file
  str_sql <- paste(readLines(loc, warn=FALSE), collapse=" \n ")
  # Clean up SQL code
  str_sql <- gsub("(/[*])(.*?)([*]/)", "", str_sql) #removes forward-slashed commented code
  str_sql <- paste(as.vector(strsplit(str_sql, ";")[[1]]), "") # creates vector of individual commands, split by ";"
  str_sql <- gsub("^[[:space:]\n[:space:]]+", "", str_sql) #removes multiple carriage returns at beginning of command
  #str_sql <- str_sql[substring(str_sql,1,2)!="--"] #remove commands starting with "--"
  str_sql <- str_sql[substring(str_sql,1,1)!=";"] #remove null commands

  return(str_sql)
}

#' Connect to EDW
#'
#' Estabilishes an ODBC connection to the EDW. If a user ID is not provided, the default account is used.
#' This function assumes you've set up an ODBC connection to EDWPRO called OR_EDWPRO.
#' If not, set up in [Control Panel]>[Administrative Tools]>[Data Sources(ODBC)]>[User DSN]
#' @param userid String value holding the user ID. Defaults to sb_sandbox_actuary
#' @param password String value holding the password for the user ID specified.
#' @keywords sql, connect, edw, odbc
#' @export
#' @examples
#' connectEDW()
connectEDW <- function(userid="sb_sandbox_actuary", password, odbc='OR_EDWPRO') {
  # Close with disconnect(connection)
  library(RODBC) #database connections
  library(getPass) #get password functionality without RStudio dependency
  if (userid!="sb_sandbox_actuary") {
    # if not default user, ask for password so that it's more secure
    #password <- invisible(.rs.askForPassword(paste0("Password for userid=", userid)))
    password <- invisible(getPass(msg = "Please enter password: "))
  } else {
    # Update this file when the sb_sandbox_actuary password changes.
    password <- read_lines("R:/EmployerGroup/SharedCode/Prod/UtilityPrograms/sb_sandbox_actuary_password.txt")
  }
  con_edw <- odbcConnect(odbc, uid=userid, pwd=password, believeNRows=FALSE, case="toupper")
  return(con_edw)
}


#' Execute SQL
#'
#' @param str_sql SQL code.
#' @param sqlFile Indicates that strSQL contains the exact location of a *.SQL file (instead of raw SQL code). Defaults to FALSE.
#' @param con ODBC connection to SQL database. Defaults to a new connections to EDW.
#' @keywords sql, query, pull
#' @export
#' @examples
#' executeSQL('SELECT 1 FROM dual')
executeSQL <- function(str_sql, sqlFile = FALSE, con = connectEDW()){
  # This function is intended to run multiple SQL statements

  library(RODBC) #database connections

  if (sqlFile) {
    # str_sql currently defines the location of the .SQL file.
    # Replace it with the actual SQL code and proceed
    str_sql <- readSQL(str_sql)
  }

  for (n in 1:NROW(str_sql)){
    # note that SQL scripts with multiple commands only return final query results
    results <- sqlQuery(con, str_sql[n], errors=FALSE)
  }
  names(results) <- tolower(names(results))
  return(results)
}

#' Query EDW
#'
#' Executes a SQL query on EDWPROD and returns the result.
#' @param str_sql SQL code.
#' @param sqlFile Indicates that strSQL contains the exact location of a *.SQL file (instead of raw SQL code). Defaults to FALSE.
#' @param con_edw ODBC connection to EDW. Defaults to a new connections to EDW.
#' @keywords sql, edw, query, pull
#' @export
#' @examples
#' queryEDW('SELECT 1 FROM dual')
queryEDW <- function(str_sql, sqlFile = FALSE, con_edw=connectEDW()) {
  # Runs SQL query on EDWPROD and returns result
  # If a connection is not provided, one will be established with the default account
  # str_sql = SQL code
  # sqlFile = indicates that strSQL contains exact location of .SQL file instead of raw SQL code

  executeSQL(str_sql, sqlFile, con = con_edw)
}


#' SQL Write Table
#'
#' Writes a data frame as a table to a server specified by the ODBC connection.
#' Basically a wrapper for RODBC::sqlSave() to ensure table names are specified in all caps to prevent unexpected "errors".
#' @param con ODBC connection to a server. Defaults to sb_sandbox_actuary account on EDWPRO
#' @param dat Data frame to be written to server.
#' @param table_name Name of table to be created. Table name must be in ALL_CAPS and alpha-numeric.
#' @param replace Should the table, if it already exists, be dropped and replaced? Defaults to FALSE. If table already exists, function will fail.
#' @param drop_table If TRUE, an attempt to drop the table is made. If the table doesn't exist, it will generate an error and continue trying to write the table.
#' @keywords sql, edw, save, write
#' @export
#' @examples
#' sqlWriteTable(con = connectEDW, dat, drop_table = TRUE)
sqlWriteTable <- function(con=connectEDW(), dat, table_name, replace = FALSE, drop_table = FALSE, dat_types = c(), ...) {
  # Writes a data frame to the server specified by the ODBC connection
  # If a connection is not provided, one will be established with the default account to EDWPRO
  # str_sql = SQL code
  # sqlFile = indicates that strSQL contains exact location of .SQL file instead of raw SQL code
  # PLEASE NOTE: Date columns must be specified with a varTypes list parameter. Column names must be in all caps.
  # varTypes = c(COV_MTH = "date")

  library(RODBC) #database connections
  library(lubridate) #is.instant() function

  dat <- as.data.frame(dat) # tbl_df sometimes causes issues
  # PL/SQL needs names to be specified in all caps for normal operation
  # This should have been specified by the connection, but just in case...
  table_name <- toupper(table_name)
  names(dat) <- toupper(names(dat))
  # Date columns must be specified as such in order to work correctly
  dat_types_names <- c()
  if (length(dat_types) == 0){
    for(i in 1:NCOL(dat)){
      if(is.instant(dat[, i])){
        dat_types <- c(dat_types, "date")
        dat_types_names <- c(dat_types_names, names(dat)[i])
      }
    }
    names(dat_types) <- dat_types_names
  }

  if (drop_table) try(sqlDrop(con, table_name))

  if (is.na(table_name)) {
    sqlSave(con, dat, safer = !replace, rownames = FALSE, fast = TRUE, varType = dat_types, ...)
  } else {
    sqlSave(con, dat, tablename = table_name, safer = !replace, rownames = FALSE, fast = TRUE,  varType = dat_types, ...)
  }
}




#### SQLite Functions ####

#' SQLite table
#'
#' Converts date values to character values in YYYY-MM-DD format.
#' This makes working with dates in SQLite easier.
#' @param dat Data Frame to be modified.
#' @keywords date, data frame, datetime, POSIXct, character, string, instant
#' @export
#' @examples
#' sqliteTable()
sqliteTable <- function(dat) {
  # SQLite can't handle date values.
  # Instead, dates are stored as strings.
  # This function returns the same data frame passed in with Date or POSIXct dates converted to characters.
  # This should be used prior to creating SQLite tables from dataframes
  library(lubridate)
  dat <- as.data.frame(dat) # tbl_df for some reason causes problems with is.instant()
  for(j in 1:NCOL(dat)){
    if (is.instant(dat[,j])) {
      dat[ ,j] <- as.character(dat[ ,j])
    }
  }
  return(dat)
}

