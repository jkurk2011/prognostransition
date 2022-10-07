#' Converts all date-time values to Dates to remove time values. This function returns the same data frame passed in with Date or POSIXct dates converted to Date.
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
