#' FIPS Choropleth
#'
#' This function takes a data frame with State, County Code and a single metric, and returns a ggplot choropleth of the United States.
#' @param base_size Determines the base size of the text in theme elements.
#' @keywords ggplot2, choropleth, map,
#' @export
#' @examples
#' fips_choropleth(dat_map, "metric")
fips_choropleth <- function(dat, str_metric, cols = getHumanaColors(5)) {
  require(ggplot2)
  require(readr)
  require(dplyr)
  require(magrittr)
  
  #dat must have state, cnty_cd, and a metric that matches str_metric
  # state is the state abbreviation in all caps
  # cnty_cd is the county code as a number (no leading zeros)
  # str_metric is the name of the column to be used for assigning color to the choropleth
  ## works best with discrete variables
  
  # Import crosswalk table
  dat_crosswalk <- read_csv("R:/EmployerGroup/SharedCode/Prod/UtilityPrograms/mapping_crosswalk.csv")
  
  # Join together
  dat_map <- left_join(
    dat,
    dat_crosswalk,
    by = c("state" = "state", "cnty_cd" = "cnty_cd")
  )
  
  # Import data used for defining map
  dat_cnty <- map_data("county")
  dat_cnty %<>% mutate(county_name = paste(region, subregion, sep = ","))
  
  dat_map %<>% left_join(dat_cnty, .,
                         by = c("county_name" = "polyname")) %>%
    arrange(order)
  dat_state <- map_data("state")
  
  return(
    ggplot(dat_map, aes(long, lat, group = group)) +
      geom_polygon(fill = "darkgrey", alpha = 0.75) +
      geom_polygon(aes_string(fill = str_metric)) +
      scale_fill_manual(values = cols) +
      coord_map(project = "globular", xlim = c(-120.7, -72.4), ylim = c(25.4, 49.3)) +
      theme(axis.title = element_blank(),
            axis.text = element_blank(),
            axis.ticks = element_blank(),
            panel.grid.major = element_blank(),
            panel.grid.minor = element_blank(),
            panel.background = element_blank(),
            legend.position = "bottom"
      ) +
      geom_path(data = dat_state, colour = "#f4f4f4", size = .7) +
      geom_path(data = dat_cnty, colour = "#f4f4f4", size = .5, alpha = .1)
  )
  
}