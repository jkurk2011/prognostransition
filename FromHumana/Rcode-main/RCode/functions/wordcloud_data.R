#' Wordcloud Data
#'
#' This function prepares a single column of word data to be used in a wordcloud plot
#' This is mainly a convenience function.
#' @param dat_text A single column of data containing words to be made into a wordcloud
#' @keywords wordcloud, corpus,
#' @export
#' @examples
#' dat_terms <- wordcloud_data(dat_sparc$Narrative)
#' # This is how you export a plot
#'  png("wordcloud.png", width=1280, height=1280, units="px", res=320, type = "cairo-png", antialias = "subpixel")
#'    # note that a wordcloud's word placement is random. You might want to run this a few times until you get one you like.
#'    wordcloud(names(dat_terms), terms, scale=c(4, 0.5),
#'              min.freq = 25, max.words=200, # these control which words make it into the plot
#'              colors=getHumanaColors()) # Use Humana colors
#'  dev.off() # Stop waiting for plots to export
wordcloud_data <- function(dat_text){
  # dat_text must be a single column of data containing words to be made into a world cloud
  
  source("R:/EmployerGroup/SharedCode/Prod/UtilityPrograms/general_functions.R") # This includes Humana colors
  library(magrittr) # This includes the %>% operator
  
  # Load packages to create a wordcloud
  require(tm)
  require(wordcloud)
  require(RColorBrewer)
  
  # Corpus ~ a collection of written texts, especially the entire works of a particular author or a body of writing on a particular subject.
  dat_corpus <- dat_text %>%
    as.data.frame() %>%
    DataframeSource() %>%
    Corpus()
  
  dat_corpus <- tm_map(dat_corpus, content_transformer(tolower)) # removes differences in capitalization
  dat_corpus <- tm_map(dat_corpus, removePunctuation) # no one cares about your punctuation
  dat_corpus <- tm_map(dat_corpus, removeNumbers) # no one cares about the numbers
  dat_corpus <- tm_map(dat_corpus, removeWords, stopwords("english")) # remove uninteresting words
  
  dat_tdm <- TermDocumentMatrix(dat_corpus, control = list(minWordLength = 1))
  dat_tdm <- as.matrix(dat_tdm)
  
  dat_terms <- sort(rowSums(dat_tdm), decreasing = TRUE) # sort words by their frequency
  return(dat_terms)
  
  # Example usage of data #
  # This is how you export a plot
  #png("wordcloud.png", width=1280, height=1280, units="px", res=320, type = "cairo-png", antialias = "subpixel")
  # note that a wordcloud's word placement is random. You might want to run this a few times until you get one you like.
  #wordcloud(names(dat_terms), terms, scale=c(4, 0.5),
  #          min.freq = 25, max.words=200, # these control which words make it into the plot
  #          colors=getHumanaColors()) # Use Humana colors
  #dev.off() # Stop waiting for plots to export
}