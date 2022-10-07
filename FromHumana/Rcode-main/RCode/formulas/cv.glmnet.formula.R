
#' Formula interface for elastic net modelling with cv.glmnet
#' @param x For the default method, a matrix of predictor variables.
#' @param formula A model formula; interaction terms are allowed and will be expanded per the usual rules for linear models.
#' @param data A data frame or matrix containing the variables in the formula.
#' @param weights An optional vector of case weights to be used in the fitting process. If missing, defaults to an unweighted fit.
#' @param offset An optional vector of offsets, an \emph{a priori} known component to be included in the linear predictor.
#' @param sparse Should the model matrix be in sparse format? This can save memory when dealing with many factor variables, each with many levels (but see the warning below).
#' @param ... For \code{glmnet.formula} and \code{glmnet.default}, other arguments to be passed to \code{\link[glmnet:glmnet]{glmnet::glmnet}}; for the \code{predict} and \code{coef} methods, arguments to be passed to their counterparts in package \code{glmnet}.
#'
#' @export
cv.glmnet.formula <- function(formula, data, weights, offset=NULL, ..., sparse = FALSE)
{
  cl <- match.call(expand.dots = TRUE)
  cl[[1]] <- quote(stats::model.frame)
  
  which = match(c("formula", "data", "weights", "offset"), names(cl), F)
  cl_form = cl[c(1,which)]
  mf <- eval.parent(cl_form)
  
  x <- if(sparse)
    dropIntercept(Matrix::sparse.model.matrix(attr(mf, "terms"), mf))
  else dropIntercept(model.matrix(attr(mf, "terms"), mf))
  y <- model.response(mf)
  weights <- model.extract(mf, "weights")
  offset <- model.extract(mf, "offset")
  if(is.null(weights))
    weights <- rep(1, length(y))
  
  
  cl[[1]] <- quote(glmnet::cv.glmnet)
  
  
  exclude <- which(names(cl) %in% c("formula", "data", "weights", "offset"))
  cl_non_form <- cl[-exclude]
  cl_non_form[[1]] <- ""
  lhs_oth <- names(cl_non_form)
  lhs_oth <- lhs_oth[-1]
  rhs_oth <- as.character(cl_non_form)
  rhs_oth <- rhs_oth[-1]
  oth_eqn <- paste(paste(lhs_oth, rhs_oth, sep="="), collapse=",")
  eqn <- paste("model <- glmnet::cv.glmnet(x, y, weights=weights, offset=offset,",oth_eqn,")")
  eval(parse(text=eqn))
  model$call <- match.call()
  model$terms <- terms(mf)
  model$sparse <- sparse
  model$na.action <- attr(mf, "na.action")
  class(model) <- c("cv.glmnet.formula", class(model))
  model
}


#' Predict function for cv.glmnet.formula
#' @param object For the \code{predict} and \code{coef} methods, an object of class \code{glmnet.formula}.
#' @param newdata For the \code{predict} method, a data frame containing the observations for which to calculate predictions.
#' @export
predict.cv.glmnet.formula <- function(object, newdata, na.action=na.pass, ...)
{
  if(!inherits(object, "cv.glmnet.formula"))
    stop("invalid glmnet.formula object")
  tt <- delete.response(object$terms)
  newdata <- model.frame(tt, newdata, na.action=na.action)
  x <- if(object$sparse)
    dropIntercept(Matrix::sparse.model.matrix(tt, newdata))
  else dropIntercept(model.matrix(tt, newdata))
  class(object) <- class(object)[-1]
  predict.cv.glmnet(object, x, ...)
}

#' Obtain coefficients from glmnet object
#' @param object For the \code{predict} and \code{coef} methods, an object of class \code{glmnet.formula}.
#' @export
coef.cv.glmnet.formula <- function(object, ...)
{
  if(!inherits(object, "cv.glmnet.formula"))
    stop("invalid glmnet.formula object")
  class(object) <- class(object)[-1]
  coef(object, ...)
}


