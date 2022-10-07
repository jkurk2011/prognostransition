#####################################################################################
# PURPOSE: Function to split the data into approximately equal groups based on
#          one variable and then plot the mean values of one or two other variables
#          against the mean of the first variable.
#          If only two variables are provided (var1 and var2) they are plotted 
#          against each other.  If three are provided than the second two are
#          plotted against the first.
#          A common use may be to plot actual vs predicted for some variable to
#          check calibration, or to plot both actual and predicted against another
#          variable to look for possible needed transformations.
#
#Most Typical Use:
#   group_and_plot_vars(my_data$prediction,
#                       my_data$actual,
#                       main='Actual vs Predicted',
#                       xlab='predicted'
#                       ylab='actual')
#Another typical use:
#    group_and_plot_vars(my_data$my_predictor,
#                        my_data$prediction,
#                        my_data$actual,
#                        square_plot=0,
#                        legend_name_var2='prediction',
#                        legend_name_var3='actual')
#####################################################################################
group_and_plot_vars <- function(
     var1, #Vector, means will be displayed on x-axis.  Will be used to group the data.
     var2, #Vector, same length as var1, means will be displayed on y-axis.
     var3=NA, #Optional vector, must be same length as var1, means displayed on y-axis.
     square_plot=1, #1-Force x- and y-axes to have exactly the same scales.  Will also
                    #  result in a 45-degree line being drawn.
                    #0-Do not force square.  X-axis and y-axes will be set independently.
                    #  Will make sure values for both var2 and var3 fit.
     groups=10, #Number of approximately equal size groups to put data in.  Will be done
                #based on quantiles, and so actual sizes of groups can end up being
                #quite different.
     legend_name_var2=deparse(substitute(var2)), #With three vars, we display a basic legend.  
                                                 #Can assign names here, else the variable
                                                 #name itself is used.
     legend_name_var3=deparse(substitute(var3)),
     legend='bottomright', #Location of legend, passed to legend function.  Only used if
                          #there are three vars.,
     ... #Can specify other plot parameters such as main, xlab, ylab
    ){

  my_seq <- seq(0,1,length.out=groups+1)
  
  #Split the data into as equal groups as possible by finding quantiles.
  quants <- quantile(var1,probs=my_seq,type=3)
  quants <- unique(quants)
  
  #Groups caculated based on var1 only and applied to var2 and var3.
  groups <- cut(var1, breaks=quants, include.lowest=T) 

  var1_grouped <- split(var1, groups) #Put variable into groups.
  var1_grouped_mean <- unlist(lapply(var1_grouped, mean))  #Calculate mean value within each group.
  var2_grouped <- split(var2, groups)
  var2_grouped_mean <- unlist(lapply(var2_grouped, mean)) 
  
  #Steps to finding min and max values for the plot axes.
  all_vars <- rbind(var1_grouped_mean,var2_grouped_mean)
  y_vars <- var2_grouped_mean

  #If var3 was provided, we need to consider it in the plot axes.  
  if(!missing(var3)){
    var3_grouped <- split(var3, groups)
    var3_grouped_mean <- unlist(lapply(var3_grouped, mean)) 
    all_vars <- rbind(all_vars,var3_grouped_mean)
    y_vars <- rbind(y_vars,var3_grouped_mean)
  }
    
  if(square_plot==1){
    max_all_vars <- max(all_vars)
    min_all_vars <- min(all_vars)
    plot(var1_grouped_mean,
         var2_grouped_mean, 
         col='blue', 
         xlim=c(min_all_vars,max_all_vars), 
         ylim=c(min_all_vars,max_all_vars),
         cex=2,
         ...
         ) 
    abline(0,1)
  }
  else{
    max_y_vars <- max(y_vars)
    min_y_vars <- min(y_vars)
    plot(var1_grouped_mean,
         var2_grouped_mean, 
         col='blue', 
         ylim=c(min_y_vars,max_y_vars),
         cex=2,
         ...
    ) 
  }
  
  if(!missing(var3)){
    points(var1_grouped_mean,var3_grouped_mean, 
           col='red', 
           cex=2)
    legend(legend,
           legend=c(legend_name_var2,legend_name_var3),
           pch=c(1,1),
           pt.cex=1,
           col=c('blue','red')
          )  
  }}