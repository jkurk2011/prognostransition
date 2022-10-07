source("S:/US/USS/ALL/Research/R_Code/prodcode/general_functions_v2.R")
# setwd("S:/US/USS/ALL/Research/Veracity Integration/Veracity Notes/VeracityModel")
library(xml2)
library(XML)
library(tidyverse)
setwd("S:/US/Studies/CHI/xml/")
all.files <-list.files(pattern="\\.xml", path = getwd(), full.names = TRUE)


ef = function(){
  df <- data.frame(0)
}
nef = function(nextfile){
  page<-read_xml(nextfile)
  ProductContractNum = xml_find_all(page, "//ProductContractNum")%>%xml_text()
  df<-data.frame(ProductContractNum)
}


readfile<-function(nextfile) {
  #read files and extract the desired nodes
t = try(read_xml(nextfile))
if(!("try-error" %in% class(t)))
{         
  page<-read_xml(nextfile)
  ProductContractNum = xml_find_all(page, "//ProductContractNum")%>%xml_text()
  df<-data.frame(ProductContractNum)
}         
} 

#get list of files and filter out xml files
# all.files <- list.files(pattern="\\.xml", path = getwd(), full.names = TRUE)
results<-lapply(all.files, readfile)

output = do.call(rbind, results)







# 
# dflist <- lapply(all.files[55080:55180], function(x){
#   # xml <- xmlParse(x)
#   xml <-read_xml(x)
#   #ProductContractNum<-xpathSApply(doc=xml,path= "//ProductContractNum",xmlAttrs)
#   # df <-data.frame(t(ProductContractNum))
#   
# })
# 
# read_xml(dflist)
# 
# data3 = xml_add_child(dflist[1],dflist[2])
# 
# formatted <-
#   data.frame(
#     ProductContractNum = xml_find_all(dflist, "//ProductContractNum")%>%xml_text()
#   )
