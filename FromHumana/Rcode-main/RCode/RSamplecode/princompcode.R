
###
#Principal components of health index data COUNTY HEALTH RANKINGS 2015 
#downloaded from http://www.countyhealthrankings.org
###
library(splines)
library(earth)

county_health_data  <-  read.csv("R:/SmallGroup/Analytics/New Case Underwriting Data Enhancement/Phase 2/HealthIndex_2015_CHR_CSV_Analytic_Data.csv", 
                               header = T, sep = ',')

column_names  <-  names(county_health_data)
column_names_last5  <-  substr(column_names, nchar(column_names)-4, nchar(column_names))

length(which(county_health_data$County_that_was_not_ranked == 1)) /dim(county_health_data)[1]
#79 counties don't have data that is about 2.5% of counties.


#exclude the 79 counties. Only include the actual values 
data_for_analysis  <-  county_health_data[-which(county_health_data$COUNTYCODE == 0),]
data_for_analysis  <-  data_for_analysis[-which(data_for_analysis$County_that_was_not_ranked == 1), 
                                        which(column_names_last5 == 'Value')]
names(data_for_analysis)

dim(data_for_analysis)
#[1] 3062   55

data_labels  <-  county_health_data[-which(county_health_data$County_that_was_not_ranked == 1),1:4]

data_labels <-  data_labels[-which(data_labels$COUNTYCODE == 0),]

data_labels$state_county  <-  with(data_labels, paste0(STATECODE, '-',COUNTYCODE))

counties.labs  <-  data_labels$state_county

## now replacing missing values by zero. May need to come back here and change it. 
data_for_analysis[is.na(data_for_analysis)]  <-  0

data_for_analysis  <-  data.matrix(data_for_analysis)




#### clustering;
#hierarchical clustering on county_health_index data
dim(data_for_analysis)
#[1] 3062   55

# PCA on the county_health_index Data

pr.out <- prcomp(data_for_analysis, scale=TRUE)

orig.pr.out <- pr.out



Cols <- function(vec){
  cols=rainbow(length(unique(vec)))
  return(cols[as.numeric(as.factor(vec))])
}

par(mfrow=c(1,2))
plot(pr.out$x[,1:2], col=Cols(counties.labs), pch=19,xlab="Z1",ylab="Z2")

plot(pr.out$x[,c(1,3)], col=Cols(counties.labs), pch=19,xlab="Z1",ylab="Z3")
summary(pr.out)
plot(pr.out)
pve <- 100*pr.out$sdev^2/sum(pr.out$sdev^2)
par(mfrow=c(1,2))

plot(pve,  type="o", ylab="PVE", xlab="Principal Component", col="blue")
plot(cumsum(pve), type="o", ylab="Cumulative PVE", xlab="Principal Component", col="brown3")

sd.data <- scale(data_for_analysis)

data.dist <- dist(sd.data)
plot(hclust(data.dist), labels=counties.labs, main="Complete Linkage", xlab="", sub="",ylab="")
plot(hclust(data.dist, method="average"), labels=counties.labs, main="Average Linkage", xlab="", sub="",ylab="")
plot(hclust(data.dist, method="single"), labels=counties.labs,  main="Single Linkage", xlab="", sub="",ylab="")
hc.out <- hclust(dist(sd.data))
hc.clusters <- cutree(hc.out,10)

table(hc.clusters)
table(hc.clusters,counties.labs)

par(mfrow=c(1,1))
plot(hc.out, labels=counties.labs)

cbind(data_labels, pr.out$x[,1:4])


####Implementing Maximum total variance method. 
#1. Compute PC1, the first PC of the variables to reduce X1,...,Xq using the correlation matrix of Xs. 
# this is done above. 

#2. Use ordinary linear regression to predict PC1 on the basis of functions of the Xs



for(reps in 1:5){
  new_data_for_analysis  <-  NULL;
  
  for(j in 1:ncol(data_for_analysis)){
    xj_basis_splines  <-   bs(data_for_analysis[,j], df = 3)  
    lm_on_pc1splines  <-  lm(pr.out$x[,1] ~  xj_basis_splines)
    new_data_for_analysis  <-  cbind(new_data_for_analysis, lm_on_pc1splines$fitted);
  }
  
  pr.out <- prcomp(new_data_for_analysis, scale=TRUE)
 
}

plot(pr.out)
pve <- 100*pr.out$sdev^2/sum(pr.out$sdev^2)
par(mfrow=c(1,2))
plot(pve,  type="o", ylab="PVE", xlab="Principal Component", col="blue")
plot(cumsum(pve), type="o", ylab="Cumulative PVE", xlab="Principal Component", col="brown3")


healthscore_by_county <- data.frame(data_labels,health_score =  pr.out$x[,1], prin_cmps = orig.pr.out$x[,1:4],data_for_analysis )

dim(healthscore_by_county)
#write.csv(healthscore_by_county, "R:/SmallGroup/Analytics/New Case Underwriting Data Enhancement/Phase 2/HealthScoreByCounty.csv")
write.csv(healthscore_by_county, "R:/EmployerGroup/Analytics/Two Plus NB Morbidity Modeling/healthdataandscorebycounty.csv")




