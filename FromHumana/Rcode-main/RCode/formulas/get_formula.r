#' Get Formula
#' 
#' A function to create an R formula from list of predictors from an excel file. 
#' @param attr_list_csv_file a \code{csv} file that has a list of predictors and response. The file should include
#' the columns \code{Attribute},	\code{Variable_Category},
#' \code{IsChar},	\code{PredictorsInVersion1}, ..., \code{PredictorsInVersion10}

#' @export
get_formula <- function(attr_list_csv_file = "ModelVersion.csv", version_name = "PredictorsInVersion1"){
  
  model_version_info <- read.csv(attr_list_csv_file, header = TRUE)
  
  var_index <- eval(parse(text = paste0("which(model_version_info$",version_name," == 1)")))
  model_version_info <- model_version_info[var_index,]
  response_var <- model_version_info$Attribute[(model_version_info$Variable_Category=="Response")]
  numeric_predictors_list <- model_version_info$Attribute[(model_version_info$Variable_Category!="Response" & 
                                                             model_version_info$IsChar != 1)]
  char_predictors_list <- model_version_info$Attribute[(model_version_info$Variable_Category!="Response" & 
                                                             model_version_info$IsChar == 1)]
  
  ret <- paste(response_var, " ~" ,paste(numeric_predictors_list, collapse = " + ") )
  if(length(char_predictors_list) > 0 ){
    char_formula <- ""
    for(i in 1:length(char_predictors_list)){
      char_formula <- paste(char_formula, " + as.factor(",char_predictors_list[i],")")
    }
    ret <- formula(paste(ret,char_formula))
  }
  return(ret)
}



