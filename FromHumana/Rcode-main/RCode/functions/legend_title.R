#' Legend Title
#'
#' This function modifies the title of a legend without having to specify a new color palette.
#' This is mainly a convenience function.
#' @param title Title of legend.
#' @keywords ggplot2, legend, guide
#' @export
#' @examples
#' ggplot(dat, aes(x = x, y = y)) + goem_point() +
#' legend_title(title = "Legend Title")
legend_title <- function(title) {
  guides(fill=guide_legend(title=title))
}