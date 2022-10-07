#' Theme - Jake Gaecke
#'
#' This function specifies a theme created by Jake Gaecke for plotting in ggplot2.
#' It is based on theme_bw() with light grey background, dashed minor grid lines, and legend defaulting to the bottom.
#' @param base_size Determines the base size of the text in theme elements.
#' @keywords ggplot2, theme, theme_bw
#' @export
#' @examples
#' ggplot(dat, aes(x = x, y = y)) + goem_point() +
#' theme_jg(base_size = 18)
theme_jg = function(base_size = 12, bg_color = "white") {
  # consider using bg_color = "#f4f4f4"
  bg_rect = element_rect(fill = bg_color, color = bg_color)
  
  theme_bw(base_size) +
    theme(plot.background = bg_rect,
          panel.background = bg_rect,
          legend.background = bg_rect,
          legend.key = element_blank(), #removes box around legend elements
          legend.position = "bottom",
          panel.grid.major = element_line(colour = "#c4c4c4", size = 0.25), #grey80
          panel.grid.minor = element_line(colour = "#dcdcdc", size = 0.25, linetype = "dashed") #grey90
    )
}