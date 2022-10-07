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