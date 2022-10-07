#' Distribution summary of list of variables in a dataframe
#' @param data the name of the dataframe containing the variables
#' @param varlist list of variable whose summary distribution is desired
#' @param filename a name of a PDF and text file that will be saved in the current working directory that includes summaries
#' @export	
getsummary <- function(data, varlist,filename,append_to_file = TRUE){
  #this function creates 
  #1. a text file that contains tabulations of the categorical variables in the list
  #2. a pdf file that contains the histograms of the numeric variables in the variable list
  #3. a plot of missings in the same pdf output as #2.
  #This function requires the packages Amelia and Hmisc
  tmp_data <- data[,which(names(data) %in% varlist)]
  tmp_contents <- contents(tmp_data)
  tmp_contents <- data.frame(tmp_contents$contents)
  tmp_cat_vars <- rownames(tmp_contents)[tmp_contents$Storage == 'character']
  tmp_num_vars <- rownames(tmp_contents)[tmp_contents$Storage != 'character']
  sink(paste0(filename,".txt"), append=TRUE)
  print( apply(data.frame(tmp_data[,names(tmp_data) %in% tmp_cat_vars]),2,table ) )
  print( summary(data[,which(names(data) %in% tmp_num_vars)]) )
  sink()
  pdf(paste0(filename,".pdf"))
   missmap(tmp_data, main=paste0(filename,"Missings Map"), 
           col=c("yellow", "black"), legend=FALSE)
  
    for(i in 1:length(tmp_num_vars)){
      tmp_var <- tmp_data[,which(names(tmp_data) %in% tmp_num_vars[i])]
      hist(tmp_var,xlab = "", main = paste0(names(tmp_data)[which(names(tmp_data) %in% tmp_num_vars[i])]))
      
      tmp_var_95pctile <- quantile(tmp_var,prob=0.95, na.rm = TRUE)
      tmp_var_5pctile <- quantile(tmp_var,prob=0.05, na.rm = TRUE)
      hist(tmp_var[(tmp_var >= tmp_var_5pctile & tmp_var <= tmp_var_95pctile)], xlab = "", 
           main = paste0(names(tmp_data)[which(names(tmp_data) %in% tmp_num_vars[i])], "-excluding-outliers"))
      
    }
  
  
  dev.off()


}



