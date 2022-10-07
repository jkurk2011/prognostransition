#' @export
dbl_sqrt_xform <- function(x, outer_coef=1, inner_inter=0, inner_coef=1){
  abs_x <- sign(x) * x
  abs_result <- outer_coef * (
    sqrt(inner_inter + inner_coef * abs_x)
    - sqrt(inner_inter)
  )
  return(sign(x) * abs_result)
}
