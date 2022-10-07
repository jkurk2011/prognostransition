#' Formula interface for elastic net modelling with glmnet
#' @param x For the default method, a matrix of predictor variables.
#' @param formula A model formula; interaction terms are allowed and will be expanded per the usual rules for linear models.
#' @param data A data frame or matrix containing the variables in the formula.
#' @param weights An optional vector of case weights to be used in the fitting process. If missing, defaults to an unweighted fit.
#' @param offset An optional vector of offsets, an \emph{a priori} known component to be included in the linear predictor.
#' @param subset An optional vector specifying the subset of observations to be used to fit the model.
#' @param na.action A function which indicates what should happen when the data contains missing values. For the \code{predict} method, \code{na.action = na.pass} will predict missing values with \code{NA}; \code{na.omit} or \code{na.exclude} will drop them.
#' @param drop.unused.levels Should factors have unused levels dropped? Defaults to \code{FALSE}.
#' @param xlev A named list of character vectors giving the full set of levels to be assumed for each factor.
#' @param sparse Should the model matrix be in sparse format? This can save memory when dealing with many factor variables, each with many levels (but see the warning below).
#' @param ... For \code{glmnet.formula} and \code{glmnet.default}, other arguments to be passed to \code{\link[glmnet:glmnet]{glmnet::glmnet}}; for the \code{predict} and \code{coef} methods, arguments to be passed to their counterparts in package \code{glmnet}.
#'
#' @export
glmnet.formula <- function(formula, data, ..., weights, offset=NULL, subset=NULL, na.action=getOption("na.action"),
                           drop.unused.levels=FALSE, xlev=NULL, sparse=FALSE)
{
    cl <- match.call(expand=FALSE)
    cl$`...` <- cl$sparse <- NULL
    cl[[1]] <- quote(stats::model.frame)
    mf <- eval.parent(cl)

    x <- if(sparse)
        dropIntercept(Matrix::sparse.model.matrix(attr(mf, "terms"), mf))
    else dropIntercept(model.matrix(attr(mf, "terms"), mf))
    y <- model.response(mf)
    weights <- model.extract(mf, "weights")
    offset <- model.extract(mf, "offset")
    if(is.null(weights))
        weights <- rep(1, length(y))

    model <- glmnet::glmnet(x, y, weights=weights, offset=offset, ...)
    model$call <- match.call()
    model$terms <- terms(mf)
    model$sparse <- sparse
    model$na.action <- attr(mf, "na.action")
    class(model) <- c("glmnet.formula", class(model))
    model
}


#' Predict function for glmnet.formula
#' @param object For the \code{predict} and \code{coef} methods, an object of class \code{glmnet.formula}.
#' @param newdata For the \code{predict} method, a data frame containing the observations for which to calculate predictions.
#' @export
predict.glmnet.formula <- function(object, newdata, na.action=na.pass, ...)
{
    if(!inherits(object, "glmnet.formula"))
        stop("invalid glmnet.formula object")
    tt <- delete.response(object$terms)
    newdata <- model.frame(tt, newdata, na.action=na.action)
    x <- if(object$sparse)
        dropIntercept(Matrix::sparse.model.matrix(tt, newdata))
    else dropIntercept(model.matrix(tt, newdata))
    class(object) <- class(object)[-1]
    predict(object, x, ...)
}

#' Obtain coefficients from glmnet object
#' @param object For the \code{predict} and \code{coef} methods, an object of class \code{glmnet.formula}.
#' @export
coef.glmnet.formula <- function(object, ...)
{
    if(!inherits(object, "glmnet.formula"))
        stop("invalid glmnet.formula object")
    class(object) <- class(object)[-1]
    coef(object, ...)
}

#' drop intercept from a matrix
#' @param matr input matrix
#'@export
dropIntercept <- function(matr)
{
  if(!is.matrix(matr))
    matr <- as.matrix(matr)
  matr[, -1, drop=FALSE]
}

