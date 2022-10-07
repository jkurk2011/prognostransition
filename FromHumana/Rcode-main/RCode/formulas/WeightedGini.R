#Here is a fake comment for testing Git
WeightedGini <- function(actual_value, weights, predicted_value){
  df = data.frame(actual_value = actual_value, weights = weights, predicted_value = predicted_value)
  df <- df[order(df$predicted_value, decreasing = TRUE),]
  df$random = cumsum((df$weights/sum(df$weights)))
  totalPositive <- sum(df$actual_value * df$weights)
  df$cumPosFound <- cumsum(df$actual_value * df$weights)
  df$Lorentz <- df$cumPosFound / totalPositive
  n <- nrow(df)
  gini <- sum(df$Lorentz[-1]*df$random[-n]) - sum(df$Lorentz[-n]*df$random[-1])
  
  return(list(gini = gini, lorentz = df$Lorentz, random = df$random))
}