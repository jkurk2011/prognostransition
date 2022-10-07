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
  
  clip <- pipe("pbcopy", "w")                       
  write.table(dat, file=clip, sep="\t", col.names=col_names, row.names=row_names)
  close(clip)
  dat # pass-through data in case you want to put this in the middle of a larger calculation
}