#' Reads a *.sas7bdat file into a data.frame/tibble.
#'
#' @param dir SAS server directory location. This can be one of several shortcuts, like "databrkr", or the directory of the "user".
#' @param file Name of the *.sas7bdat file to be imported. This should not include the ",sas7bdat" extension.
#' @param userid User ID used to login to the SAS server. This function will prompt for the password. If dir = "user" then the user's own data folder will be used as the import directory.
#' @param password The password that corresponds to the userid. Normally, this should not be specified, which forces the function to prompt for the password. However, it could be useful to safely specify the password once prior to calling this function multiple times.
#' @param other_userid Use this to specify a directory for a userid that isn't used as the login credentials.
#' @keywords sas, sas7bdat, import, read
#' @export
#' @examples
#' dat_temp <- sas_read("user", "cbsatocountycrosswalk", "jxg9620")
#'
#' dat_temp <- sas_read("other_userid", "cbsatocountycrosswalk", "jxg9620", other_userid = "jxg9620")
#' dat_temp <- sas_read("this can be ignored in this circumstance", "cbsatocountycrosswalk", "jxg9620", other_userid = "jxg9620")
sas_read <- function(dir = "sftp://sasgrid/basedata/", file = "", userid, password, other_userid = ""){
  # dir is the server directory in which the file is located
  # file is assumed to have the .sas7bdat extension
  library(haven)
  library(RCurl)
  library(getPass)
  library(magrittr)
  library(dplyr)
  # Safely get password
  if(missing(password)){
    password <- invisible(getPass(msg = "Please enter password: "))
  }
  url <-
    case_when(
      dir == "databrkr" ~ "sftp://sasgrid/basedata/datalga/databrkr/",
      dir == "datasga" ~ "sftp://sasgrid/basedata/datasga/",
      dir == "datamodl" ~ "sftp://sasgrid/basedata/datamodl/",
      dir == "user" ~ paste0("sftp://sasgrid/basedata/sasuser1/u", userid, "/"),
      #dir == "other_userid" ~ paste0("sftp://sasgrid/basedata/sasuser1/u", other_userid, "/"),
      #!missing(other_userid) ~ paste0("sftp://sasgrid/basedata/sasuser1/u", other_userid, "/"),
      TRUE ~ dir
    ) %>%
    paste0(file, ".sas7bdat")

  getBinaryURL(url, userpwd = paste0(userid, ":", password)) %>%
    read_sas() %>%
    return()
}

