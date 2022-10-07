#' Calculates a summary metric for a predictive model similar to r-squared or absolute value error.  However, it includes the concept
#' that it is better to usually predict somewhat closer to the actual vs a competing prediction than it is get very closer to the
#' actual on a small number of outliers.
#' 
#' The function calculates the total weighted loss (or error) for a prediction where that loss is essentially the absolute value 
#' deviation of the prediction vs. the actual, with the loss downweighted based on how much closer the prediction is to the actual 
#' than the competing prediction.  The downweighting is done using an "elasticity" function, but made to be two-sided so that it 
#' doesn't matter in what direction the errors take place--that is, it isn't "better" to be lower than the competing prediction unless 
#' that gets us closer to the actual.  This metric makes it more important to be closer to the actual than the competing prediction, 
#' and less important to truly get close to the actual.  Thus, it balances elements of ranked correlation and elements of raw 
#' correlation.  The standard output is a one-row dataframe that contains several summary metrics.  The main metric, though is the 
#' total weighted loss, which gets compared to the null loss that is based on using the grand mean as the prediction.  We want 
#' pred1_loss_pct to be as low as possible.
#' 
#' The essential steps in the calculation are:
#' 1) Calculate for each observation the loss, or absolute deviation of the prediction from the actual.
#' 2) If pred1 (the prediction we care about) is closer to the actual than pred2, downweight the loss based on a mirror of the lower
#'    half of a logistic curve.  This means the loss will reduce faster near pred2 than far from pred2.  Set a floor to the weight so 
#'    that the loss doesn't completely vanish before the prediction equals the actual.
#' 3) Calculate the total downweighted loss.  While we do divide by weight, which represents exposure units, we don't divide by the
#'    elasticity downweight because that is intended to actually reduce total losses.
#' 4) Compare to null loss that would be experienced if the grand mean were predicted for all.
#' 5) Perform steps 1-4 both with and without rebalancing the predictions, and use the worse of the two.
#' 
#' @examples
#' elas_loss(
#'     data=my_data,
#'     pred1='my_new_prediction',
#'     actual='my_actual_future_claims',
#'     weight='member_months'
#'     )
#'
#' Main change that might need to be made is passing in pred2 if we want to see how a new prediction fairs when competing
#' against an existing one, rather than against the grand mean.
#' @param data A data frame containing pred1, pred2, actual, and weight columns, as applicable.
#' @param pred1 Quoted string, name of the field containing the prediction we want to test.
#' @param pred2 Quoted string, name of field containing the competing prediction.  Default is the grand mean of actual claims.  This prediction does not get directly evaluated, but is only used to see how pred1 compares against it for each group.
#' @param actual Quoted string, name of field containing the actual claims.
#' @param weight Quoted string, name of field containing weights for each observation; e.g., member months or premium.  Default is equal weights.
#' @param elas_slope Slope of a logit elasticity curve that compares two competing premiums.  However, it is used in a two-sided manner, so that it is just as bad to be below as above the competing prediction.
#' @param ret_impact_only If true, return only the savings as a scalar, rather than the dataframe of results.  Good for optimization.
#'
#' @export
elas_loss <- function(data, pred1, pred2=0, actual, weight=0, elas_slope=-6, prob_min=0.1, ret_impact_only=F) {
  
  require(magrittr)
  require(dplyr)
  
  temp_dat <- data.frame(pred1=data[[pred1]])
  temp_dat$actual <- data[[actual]]
  if(weight==0) temp_dat$weight <- 1 #If no weight provided, use weight of 1 for all observations.
  else temp_dat$weight <- data[[weight]]
  if(pred2==0) temp_dat$pred2 <- with(temp_dat, sum(actual * weight) / sum(weight)) #If no pred2 given, use mean of actual claims.
  else temp_dat$pred2 <- data[[pred2]]
  
  temp_dat %<>% mutate(
    pred1_bal = pred1 * sum(actual * weight) / sum(pred1 * weight), 
    pred2_bal = pred2 * sum(actual * weight) / sum(pred2 * weight)
    )

  #Main function that does most of the work.  We calculate the loss reduction both with and without rebalancing the prediction to equal the
  #actual, and then use whichever is more pessimistic.
  calc_loss <-function(data, use_bal){
    
    if(use_bal){
      data %<>% mutate(
        pred1_use = pred1_bal,
        pred2_use = pred2_bal
      )
    } else {
      data %<>% mutate(
        pred1_use = pred1,
        pred2_use = pred2
      )
    }
  
    #We want the loss to get downweighted only to the extent that it is CLOSER to the actual than the competing prediction.
    data %<>% mutate(
      use_rel = ifelse(pred1_use >= pred2_use & pred2_use >= actual, 1,
                     ifelse(pred1_use >= actual & pred2_use <= actual, actual / pred2_use,
                     ifelse(pred1_use >= pred2_use & pred1_use <= actual, pred1_use / pred2_use,
                     ifelse(pred1_use <= pred2_use & pred2_use <= actual, 1,
                     ifelse(pred1_use <= actual & pred2_use >= actual, pred2_use / actual,
                     ifelse(pred1_use <= pred2_use & pred1_use >= actual, pred2_use / pred1_use, 1
                            )))))),
      elas_wt = prob_min + 1 / (1 + exp(elas_slope - use_rel * elas_slope)), #Parity will produce 50% plus prob_min.  Deviations will be downweighted.  No requirement for the weights to average up to any particular number.
      null_loss = abs( sum(actual * weight) / sum(weight) - actual),
      pred1_loss = abs(pred1_use - actual)
  )  

  temp_sum <-
    data %>% dplyr::summarize(
      pred1_avg = sum(pred1 * weight) / sum(weight),
      pred2_avg = sum(pred2 * weight) / sum(weight),
      actual_avg = sum(actual * weight) / sum(weight),
      elas_wt_avg = sum(elas_wt * weight) / sum(weight),
      null_loss_avg = sum(null_loss * weight * (0.5 + prob_min)) / sum(weight),
      pred1_loss_avg = sum(pred1_loss * weight * elas_wt) / sum(weight), #Purposely exclude elas_wt from denominator, as we want it to actually bring the loss down.
      pred1_loss_pct = pred1_loss_avg / null_loss_avg
      )
  return(temp_sum)  
  }
  
  raw_sum <- calc_loss(temp_dat, use_bal=F)
  bal_sum <- calc_loss(temp_dat, use_bal=T)
  
  if(bal_sum$pred1_loss_pct > raw_sum$pred1_loss_pct) use_sum <- bal_sum
  else use_sum <- raw_sum
  
  if(ret_impact_only) return(use_sum$pred1_loss_pct)
  else return(use_sum)
}


