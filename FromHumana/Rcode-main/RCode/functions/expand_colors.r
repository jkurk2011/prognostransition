#' Expand Colors
#'
#' Takes a vector of colors and returns a vector of (at least) n colors per original color that are equally lighter and darker than the original.
#' This function will always output at least the original colors.
#' @param col A vector of colors.
#' @param n The number of colors to be output. If n is even, this function will output an additional color in order to include the original as the center color.
#' @keywords colors, shades, lighter, darker
#' @export
#' @examples
#' expand_colors(getHumanaColors(), 2) # This will add a single lighter and darker version of each color in the original vector
expand_colors <- function(col, n) {
  
  cols <- c()
  n_half <- ceiling((n-1)/2)
  n <- 1 + n_half * 2
  col_offset <- c(1/n, 1/n, 1/n, 0)
  
  
  #generate lightest colors first
  if(n_half > 0){
    for( i in n_half:1) {
      col_adj <- adjustcolor(col, offset = col_offset * i * 1)
      cols <- na.omit(rbind(cols, col_adj, deparse.level = 0))
    }
  }
  # original color
  cols <- na.omit(rbind(cols, col, deparse.level = 0))
  # generate darker colors last
  if(n_half > 0){
    for(i in 1:n_half) {
      col_adj <- adjustcolor(col, offset = col_offset * i * -1)
      cols <- na.omit(rbind(cols, col_adj, deparse.level = 0))
    }
  }
  return(as.vector(cols))
}