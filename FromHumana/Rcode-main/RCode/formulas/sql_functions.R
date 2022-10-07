#' Reads a *.SQL file into a vector of strings.
#' SQL commands are automatically split by ";". SQL commands starting with "--" are not removed, since scripts sometimes starts with a comment. However, SQL commands between /* SQL CODE */ are removed. NULL SQL commands are also removed.
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


#' Estabilishes an ODBC connection to the EDW. If a user ID is not provided, the default account is used.
#' This function assumes you've set up an ODBC connection to EDWPRO called OR_EDWPRO.
#' If not, set up in [Control Panel]>[Administrative Tools]>[Data Sources(ODBC)]>[User DSN]
#' @param userid String value holding the user ID. Defaults to sb_sandbox_actuary
#' @param password String value holding the password for the user ID specified.
#' @keywords sql, connect, edw, odbc
#' @export
#' @examples
#' con_edw <- connectEDW()
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


#' Executes SQL query
#' @param str_sql SQL code.
#' @param sqlFile indicates that strSQL contains the exact location of a *.SQL file (instead of raw SQL code). Defaults to FALSE.
#' @param con ODBC connection to SQL database. Defaults to a new connections to EDW.
#' @keywords sql, query, pull
#' @export
#' @examples
#' executeSQL("SELECT 1 FROM dual")
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
#' sqlWriteTable(con = connectEDW(), dat, table_name = "testing_table", drop_table = TRUE)
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
