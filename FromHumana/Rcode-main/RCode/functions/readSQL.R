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