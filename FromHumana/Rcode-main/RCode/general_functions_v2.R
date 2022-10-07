#### General Functions ####
# Description: This file sources custom functions that have been written to simplify common tasks.

# This is a workaround that avoids a package installation issue on Windows
#trace(utils:::unpackPkgZip, quote(Sys.sleep(2)), 
#      at = which(grepl("Sys.sleep", body(utils:::unpackPkgZip), fixed = TRUE)))   

# Load Libraries
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/loadLibraries.R")
loadLibraries() # loads default libraries

# Classification Metrics
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/class_metrics.R")

# Copy To Clipboard
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/copyToClipboard.R")

# Connect to EDW
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/connectEDW.R")

# CV.GLMNET Modelling Formula
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/cv.glmnet.formula.R")

# Double Squareroot Xform
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/dbl_sqrt_xform.R")

# Execute SQL
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/executeSQL.R")

# Fit Metrics
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/fit_metrics.R")

# Fix Dates
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/fixDates.R")

# Get Formula
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/get_formula.R")

# Get Summary
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/getsummary.R")

# GLMNET Modelling Formula
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/glmnetFormula.R")

# Query EDW
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/queryEDW.R")

# Read SQL
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/readSQL.R")

# SAS Read
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/sas_read.R")

# SQL Write Table
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/sqlWriteTable.R")

# SQLite table
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/sqliteTable.R")


#### Plotting Functions ####

# Expand Colors
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/expand_colors.R")

# FIPS Choropleth
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/fips_choropleth.R")

# Legend Title
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/legend_title.R")

# Theme - jg
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/theme_jg.R")

# Title with Subtitle
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/title_with_subtitle.R")

# Wordcloud Data
source("/Users/patrickkurkiewicz/Desktop/Transferred Code/Rcode-main/RCode/functions/wordcloud_data.R")
