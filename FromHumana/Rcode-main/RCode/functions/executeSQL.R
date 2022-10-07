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