# Classification Metrics
require(caret)
require(pROC)
require(ggplot2)
require(dplyr)
require(magrittr)

class_metrics <- function(model, data, actual, positive_class,
                          plot_one_var = c(), binary_cutoff = .5){
  
  
  #factor levels
  data$actual_class <- pull(data[actual])
  #find Reference Level
  reference_level <- levels(factor(data$actual_class,
                                   ordered = FALSE))[which(levels(factor(data$actual_class,
                                                                         ordered = FALSE)) != positive_class)]

  data$actual_class <- relevel(factor(data$actual_class, ordered = FALSE), ref = reference_level)
  
  
  #Get Predicted Probabilities
  data$class_prob <-  predict(model, data, type = 'prob')[,2]
  
  
  #Get Binary Predictions
  data$class_pred <- ifelse(data$class_prob >= binary_cutoff,
                            positive_class, reference_level)
  
  
  
  
  #Create Confusion Matrix
  cm <- confusionMatrix(as.factor(data$class_pred), 
                        data$actual_class,
                        positive = positive_class)
  
  
  #Create ROC Curve
  
  
  roc_data <- roc(data$actual_class, data$class_prob, quiet = TRUE)
  
  sen_spec <- data.frame(spec = roc_data$specificities,
                         sen = roc_data$sensitivities)
  
  df_thresh <- data.frame(sen = roc_data$sensitivities, 
                   spec = roc_data$specificities, 
                   sum = roc_data$sensitivities + roc_data$specificities, 
                   thresh = roc_data$thresholds)
  
  df_thresh %<>%
    filter(sum == max(sum))
    
  roc_curve <- ggplot(sen_spec, aes(1-spec, sen))+
                  geom_abline(color = 'red')+
                  geom_line()+
                  geom_ribbon(aes(x = 1-spec, ymin = 0, ymax = sen),
                              alpha = .4, fill = 'green')+
                  geom_ribbon(aes(x = 1-spec, ymin = sen, ymax = 1),
                              alpha = .4, fill = 'red')+
                  geom_point(data = df_thresh, aes(x = 1-spec, y = sen),
                             color = '#00BFC4', size = 2)+
                  theme_bw()+
                  xlab('1-Specificity')+
                  ylab('Sensitivity')+
                  ggtitle(paste('Area Under ROC Curve:', round(roc_data$auc,3),
                                ' Optimal Threshold = ',
                                as.character(round(df_thresh$thresh,3))))
    
    
    
    
    
  if(length(plot_one_var) >0){
     #examine one Variable
  var_plots <- list()
  for(i in 1:length(plot_one_var)){
    
    x <- pull(data[plot_one_var[i]])
    y <- data$class_prob
    class <- data$actual_class
    
    plot_data <- data.frame(x = x, y = y, class = class)
    
    var_plots[[plot_one_var[i]]] <- ggplot(plot_data, aes(x,y, color = class))+
                                      geom_point(alpha = .3)+
                                      geom_hline(yintercept = df_thresh$thresh,
                                               color = 'green', linetype = 'dashed')+
                                      geom_hline(yintercept = binary_cutoff,
                                                 color = 'red', linetype = 'dashed')+
                                      theme_bw()+
                                      xlab(plot_one_var[i])+
                                      ylab('Predicted Probability')+
                                      labs(caption = 'Chosen Cutoff in Red, Optimal Cutoff in Green')
    
  
    names(var_plots)[i] <- plot_one_var[i]
  } 
  }

  
  
  #drop column from data
  data <- data[, !colnames(data) %in% 'actual_class']
  
  #return everything as a list
  if(length(plot_one_var) >0){
    return(list(data = data, cm = cm, roc_curve = roc_curve,
                variable_plots = var_plots, optimal_threshold = df_thresh))
  }else{
   return(list(data = data, cm = cm, roc_curve = roc_curve,
               optimal_threshold = df_thresh)) 
  }
  
  
  
}