#' Model claims savings summary metric function.
#'
#' General function for calculating a consistent, proxy claims savings due to using one prediction to price vs. another.  Intended
#' to be used as a summary metric like r-squared and absolute value error, but this metric more closely represents the impact our
#' models actually have on results.  Unlike r-squared, it should not be as sensitive too outliers.  But unlike a rank
#' correlation, it still matters what the actual point estimate of the prediction is.
#' The basic functionality is that it compares pred1 and pred2 to each other and estimates the likelihood of either one winning a 
#' case using a logistic elasticity.  The claims savings is how much lower the probability weighted ACTUAL claims are to the raw
#' actual claims.  We also have the option to adjust for any difference in bias between the two predictors, which can come into
#' play when a validation dataset is being used, or when the prediction is carried over from a prior modeling exercise.
#' The standard output is a one-row dataframe that contains several summary metrics including the savings for both pred1 an pred2.
#' This can be modified to return only the pred1 savings as a scalar, which can be useful for running optimize or in custom loops.
#' @param data A data frame containing pred1, pred2, actual, and weight columns, as applicable.
#' @param pred1 Quoted string, name of the field containing one of the predictions used for pricing.
#' @param pred2 Quoted string, name of field containing the second prediction hypothetically being used for pricing.  Default is the grand mean of actual claims.
#' @param actual Quoted string, name of field containing the actual claims.
#' @param weight Quoted string, name of field containing weights for each observation; e.g., member months or premium.  Default is equal weights.
#' @param avg_prob The average probability the event of interest.  E.g., .05 for quotes and 0.85 for renewals.  The impact of competitiveness differences on the elasticity will be largest at 0.5.  But the impact on weighted claims may be larger at low percentages.  Default is 0.5. 
#' @param elas_slope Slope of a logit elasticity curve that compares two competing premiums.  Default slope is based on the 51-99 survival modeling, translated from a probit model to a logit.
#' @param pct_of_group What portion of the group does one observation typically represent.  Is 1.0 if we're modeling at the group level, but might only be 0.01 if we're modeling at the member level.  It matters because predicting one member is only going to have so much impact on the entire group's premium.
#' @param use_bias_penal If true, will penalize the claims savings by the amount the mean of one pred is off MORE than the other.
#' @param ret_impact_only If true, return only the savings as a scalar, rather than the dataframe of results.  Good for optimization.
#' @export
#' @examples
#' Example recommended STANDARDIZED call that can be used to compare predictive power between case sizes, data sets, etc.
#'
#' model_clms_save(
#'     data=my_data,
#'     pred1='my_new_prediction',
#'     actual='my_actual_future_claims',
#'     weight='member_months'
#'     )
#'
#' Example recommended CUSTOM call that might give a more realistic estimate of the impact on claims.  This example assumes
#' a member level for a 51-99 new business model, and that competitor is using GRx.
#'
#' model_clms_save(
#'     data=my_data,
#'     pred1='my_new_prediction',
#'     pred2='grx',
#'     actual='my_actual_future_claims',
#'     weight='member_months',
#'     avg_prob=0.2,
#'     pct_of_group=0.01
#'     )
model_clms_save <- function(data, pred1, pred2=0, actual, weight=0, avg_prob=0.5, elas_slope=-13, pct_of_group=1, use_bias_penal=T, ret_impact_only=F) {
  
  require(magrittr)
  require(dplyr)
  
  temp_dat <- data.frame(pred1=data[[pred1]])
  temp_dat$actual <- data[[actual]]
  if(weight==0) temp_dat$weight <- 1 #If no weight provided, use weight of 1 for all observations.
  else temp_dat$weight <- data[[weight]]
  if(pred2==0) temp_dat$pred2 <- with(temp_dat, sum(actual * weight) / sum(weight)) #If no pred2 given, use mean of actual claims.
  else temp_dat$pred2 <- data[[pred2]]
  
  temp_dat %<>% mutate(
    pred1_bal = pred1 * sum(actual * weight) / sum(pred1 * weight), #Balance the preds to the actual before comparing.  We will penalize for bias later, if desired.
    pred2_bal = pred2 * sum(actual * weight) / sum(pred2 * weight),
    pred1_grp = pct_of_group * pred1_bal + (1 - pct_of_group) * sum(actual * weight) / sum(weight), #Imapct of one member on the group's premium.
    pred2_grp = pct_of_group * pred2_bal + (1 - pct_of_group) * sum(actual * weight) / sum(weight),
    pred1_v_pred2 = (pred1_grp / pred2_grp),
    pred1_odds = exp(12 + pred1_v_pred2 * elas_slope), #Odds ratio due to the elasticity.  No intercept included, so cannot be used directly yet.
    pred2_odds = 1 / pred1_odds
  )
  
  #Helper function to solve for the intercept term needed to make sure our average probability matches the function argument.
  odds_grossup <- function(adj, odds, weight, avg_prob){
    odds_bal <- odds * adj
    prob_bal <- odds_bal / (1 + odds_bal)
    avg_prob_bal <- sum(prob_bal * weight) / sum(weight)
    diff <- (avg_prob_bal - avg_prob)^2
    return(diff)
  }
  
  opt_adj1 <- optimize(odds_grossup, lower=0.005, upper=200, odds=temp_dat$pred1_odds, weight=temp_dat$weight, avg_prob=avg_prob)[1]
  opt_adj2 <- optimize(odds_grossup, lower=0.005, upper=200, odds=temp_dat$pred2_odds, weight=temp_dat$weight, avg_prob=avg_prob)[1]
  opt_adj1 <- as.numeric(opt_adj1)
  opt_adj2 <- as.numeric(opt_adj2)
  
  temp_dat %<>% mutate(
    pred1_odds_bal = pred1_odds * opt_adj1,
    pred2_odds_bal = pred2_odds * opt_adj2,
    pred1_prob = pred1_odds_bal / (1 + pred1_odds_bal),
    pred2_prob = pred2_odds_bal / (1 + pred2_odds_bal)
    )
  
  temp_sum <-
    temp_dat %>% summarize(
      pred1_avg = sum(pred1 * weight) / sum(weight),
      pred2_avg = sum(pred2 * weight) / sum(weight),
      actual_avg = sum(actual * weight) / sum(weight),
      pred1_prob_pred = sum(pred1_prob * weight * pred1_bal) / sum(pred1_prob * weight), #Produced for informational purposes, but not used.  
        #Theory is that if we have the ability to select the best risk, we can always decide not to go all the way down in some cases.
      pred2_prob_pred = sum(pred2_prob * weight * pred2_bal) / sum(pred2_prob * weight),
      pred1_prob_clms = sum(pred1_prob * weight * actual) / sum(pred1_prob * weight), #Probability weighted claims, put back on original scale for ease of understanding.
      pred2_prob_clms = sum(pred2_prob * weight * actual) / sum(pred2_prob * weight)
    ) %<>% mutate(
    pred1_abs_diff = abs(pred1_avg - actual_avg)/actual_avg,
    pred2_abs_diff = abs(pred2_avg - actual_avg)/actual_avg,
    pred1_clms_impact_raw = pred1_prob_clms / actual_avg - 1,
    pred2_clms_impact_raw = pred2_prob_clms / actual_avg - 1,
    pred1_clms_impact = pred1_clms_impact_raw + (pred1_abs_diff - pred2_abs_diff) * ifelse(use_bias_penal,1,0), #Optionally add bias penalty.
    pred2_clms_impact = pred2_clms_impact_raw + (pred2_abs_diff - pred1_abs_diff) * ifelse(use_bias_penal,1,0)
    )
  
  if(ret_impact_only) return(temp_sum$pred1_clms_impact)
  else return(temp_sum)
}
  