#' Calculates one or more fit metrics for a continuous prediction and corresponding actual.  
#' 
#' In particular, rsq (r-squared), msep (mean squared error expressed as a percentage of the mean actual), and 
#' maep_adj (mean absolute error, expressed as a percentage of the actual, and adjusted for bias) are all produced by
#' default.  
#' 
#' The function also produces pred_margin_improve, which is the profit margin improvement that can be expected.  This
#' of course is only strictly applicable in the case of a new busines, group-level future claims prediction being used in 
#' underwriting, though it may be a useful summary in other cases as well because it combines the three default model fit 
#' metrics in an apples-to-apples manner.  Specifically, each of the model fit metrics is run through a quadratic regression 
#' that was fit to margin improvement via a simulation study.  Then the three estimates are blended together.
#' 
#' Intermediate metrics (including, for example, regular mean absolute error) can be obtained by using return_list = 'all'
#' or a vector containing all of the specific metrics desired.
#' 
#' @examples
#' fit_metrics(
#'     data=my_data,
#'     pred='p_fut_claims',
#'     actual='fut_claims',
#'     weight='member_months'
#'     )
#' 
#' fit_metrics(
#'     data=my_data,
#'     pred='p_fut_claims',
#'     actual='fut_claims',
#'     partition = 'partition',
#'     partition_to_rebal = 'train',
#'     return_list = c('rsq', 'maep_adj')
#'     )
#' 
#'
#' @param data A data frame containing the desired columns.
#' @param pred Quoted string, name of the field containing the prediction we want to evaluate.
#' @param actual Quoted string, name of field containing the actual.
#' @param weight Quoted string, name of field containing weights for each observation; e.g., member months or premium.  Default is equal weights.
#' @param partition Quoted string, name of field containing the partition into training, validation, and/or test.  In actuality, this can be any column we want to group by.  If left blank there will be no grouping.
#' @param partition_to_rebal Quoted string for the VALUE of the partition field that we want to use to rebalance the predictions.  Default partition value is 'train', but this will have no effect until add_rebal = 1 or mult_rebal = 1.
#' @param add_rebal Should the prediction be shifted additively so as to correctly predict the actual on average on the partition_to_rebal?  The resulting intercept will also be used on the validation data--it will not be refit.
#' @param mult_rebal Should the prediction be shifted multiplicatively?  If add_rebal=0 and mult_rebal=1 this is just uses a multiplicative factor to force the prediction to equal the actual on average.  add_rebal=1 and mult_rebal=1 is sort of like refitting the model.  This will probably only be appropriate if the predictor was fit on some completely different dataset or isn't even on the same scale as the actual.  In any case, the fitting will only be done on the partition_to_rebal.
#' @param return_list Vector of one or more quoted column names to return.  Use 'all' to return all columns, including intermediate calculations.  Default is rsq, msep, maep_adj, and pred_margin_improve.  If a single column name is passed and there is no partition field, the function will return the scalar value of the single requested metric.
#' @param marg_blend Vector of three values for blending together estimates of margin improvement based on r-squared, mean squared error as a percentage of the mean actual, and mean absolute error as a percentage of the mean actual.
#'
#' @export

fit_metrics <- function(data, pred, actual, weight=0, partition=0, partition_to_rebal='train', add_rebal=0, mult_rebal=0,
                        return_list=c('rsq','msep','maep_adj','pred_margin_improve'), marg_blend=c(0.4, 0.2, 0.4), na.rm=T,
                        print_summary = TRUE) {
  
  require(magrittr)
  require(dplyr)
  
  temp_dat <- data.frame(pred=data[[pred]]) # Create data.frame with the prediction column specified.
  temp_dat$actual <- data[[actual]]

  if(weight==0) temp_dat$weight <- 1 #If no weight provided, use weight of 1 for all observations.
  else temp_dat$weight <- data[[weight]]

  if(partition==0) temp_dat$partition <- 'train' # If no partition provided, assign all as 'train'.  If rebal or reslope are set to 1, this ensures that the operations can be performed.
  else temp_dat$partition <- data[[partition]]
  
  temp_dat %<>% filter(!is.na(pred) & !is.na(actual) & !is.na(weight) & !is.na(partition))
  
  # Rebalance prediction if desired, based on the specified partition (normally the training data).  This step will have no
  # effect if no such partition was specified or exists.
  
  train_dat <- temp_dat %>% filter(partition==partition_to_rebal)
  
  if (add_rebal==1 && mult_rebal==0) {
    rebal_lm = lm(actual ~ 1 + offset(pred), data=train_dat, weights = weight)
    temp_dat$pred = predict(rebal_lm, newdata=temp_dat, type='response')
    if(print_summary == TRUE){
      print(summary(rebal_lm))
    }
    
  } 
  # Need special case for this, as fitting without an intercept does not guarantee that the average pred equals the average actual.
  else if (add_rebal==0 && mult_rebal==1) {
    temp_dat$pred = temp_dat$pred * sum(train_dat$actual * train_dat$weight) / sum(train_dat$pred * train_dat$weight)
  }
  else if (add_rebal==1 && mult_rebal==1) {
    rebal_lm = lm(actual ~ 1 + pred, data=train_dat, weights = weight)
    temp_dat$pred = predict(rebal_lm, newdata=temp_dat, type='response')
    if(print_summary == TRUE){
      print(summary(rebal_lm))
    }
  }
  
  data_sum <-
    temp_dat %>% 
    group_by(partition) %>%
    summarise(
      count = n(),
      mean_actual = sum(actual * weight) / sum(weight),
      mean_pred = sum(pred * weight) / sum(weight),
      mae = sum( abs(actual - pred) * weight ) / sum(weight), # Mean absolute error
      maep = mae / mean_actual, # As percentage of the actual, to make it more meaningful between contexts.
      mae_at_actual = sum( abs(actual - mean_actual) * weight ) / sum(weight),
      mae_at_pred = sum( abs(actual - mean_pred) * weight ) / sum(weight),
      # MAE tends to favor predictions that are too low, because it is maximized at the median.  So we calculate an adjusted
      # version that undoes this favoritism and penalizes for bias.
      maep_adj = maep + (mae_at_actual - mae_at_pred) / mean_actual, 
      maep_adj = ifelse(maep_adj < maep, maep, maep_adj),
      ess = sum( (actual - pred)^2 * weight ),
      tss = sum( (actual - mean_actual)^2 * weight ),
      mse = ess / sum(weight),
      # MSE is MOSTLY just a different way to expres R-squared.  For a particular set of data, they will both be optimized at
      # exactly the same point.  However, there are certain circumstances in which R-squared can behave oddly between contexts.
      # So we want to look at MSE as well.  However, MSE has little intuitive meaning.  Thus we convert it to a basis similar to
      # MAEP_ADJ above--expressed as a percentage deviation from the actual mean.
      msep = sqrt(mse) / mean_actual,
      rsq = 1 - ess / tss,
      # Comes from regression modeling in R:\EmployerGroup\Analytics\Generalized Impact\vendor_ordering_sweet_spot\code\impact_analysis_main.R
      rsq_pred_margin_improve = -0.05521 + 0.60256 * rsq - 0.18725 * rsq^2,
      msep_pred_margin_improve = 0.33519 + 0.39057 * msep - 2.18478 * msep^2,
      maep_pred_margin_improve = 0.33942 + 0.40321 * maep_adj - 3.61882 * maep_adj^2,
      pred_margin_improve = (marg_blend[1] * rsq_pred_margin_improve +
                             marg_blend[2] * msep_pred_margin_improve +
                             marg_blend[3] * maep_pred_margin_improve
                            ) / sum(marg_blend)
    )
  
  data_sum$pred_name = pred
  
  if(return_list[1]=='all') return(data_sum)
  else if(partition!=0) return(data_sum[,c('partition',return_list)])
  else if(length(return_list) > 1) return(data_sum[return_list])
  else return(as.numeric(data_sum[return_list]))
}


