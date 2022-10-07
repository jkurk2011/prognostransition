#' Converts date values to character values in YYYY-MM-DD format.
#' This makes working with dates in SQLite easier (possible).
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
