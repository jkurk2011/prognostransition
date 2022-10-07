#' Connect to EDW
#'
#' Estabilishes an ODBC connection to the EDW. If a user ID is not provided, the default account is used.
#' This function assumes you've set up an ODBC connection to EDWPRO called OR_EDWPRO.
#' If not, set up in [Control Panel]>[Administrative Tools]>[Data Sources(ODBC)]>[User DSN]
#' @param userid String value holding the user ID. Defaults to sb_sandbox_actuary
#' @param password String value holding the password for the user ID specified.
#' @keywords sql, connect, edw, odbc
#' @export
#' @examples
#' connectEDW()
connectEDW <- function(userid="sb_sandbox_actuary", password, odbc='OR_EDWPRO') {
  # Close with disconnect(connection)
  library(RODBC) #database connections
  library(getPass) #get password functionality without RStudio dependency
  if (userid!="sb_sandbox_actuary") {
    # if not default user, ask for password so that it's more secure
    #password <- invisible(.rs.askForPassword(paste0("Password for userid=", userid)))
    password <- invisible(getPass(msg = "Please enter password: "))
  } else {
    # Update this file when the sb_sandbox_actuary password changes.
    password <- read_lines("R:/EmployerGroup/SharedCode/Prod/UtilityPrograms/sb_sandbox_actuary_password.txt")
  }
  con_edw <- odbcConnect(odbc, uid=userid, pwd=password, believeNRows=FALSE, case="toupper")
  return(con_edw)
}