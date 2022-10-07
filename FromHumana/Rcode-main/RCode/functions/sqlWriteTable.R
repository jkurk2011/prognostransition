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