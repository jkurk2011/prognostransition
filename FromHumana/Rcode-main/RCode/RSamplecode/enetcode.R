library("UtilityFunctions")

cust_data  <- read_sas("J:/datalga/databrkr/nbmorb_group_data.sas7bdat")

tdata = cust_data[cust_data$partition=='train' & cust_data$subs>=10,]
vdata = cust_data[cust_data$partition=='valid' & cust_data$subs>=10,]
rm(cust_data)


############################################################################################################################################
############################################################################################################################################
############################################################################################################################################

tomatch = c("grp_ag_over_man_clm_m","^al","^zp","^hh_","^ch_","^pct_","^per_","mbr_stepwise_score_mean","mbr_forest_score_mean",	"zip_forest_score_mean")
varlist=colnames(tdata[,grep(paste(tomatch,collapse="|"),colnames(tdata),ignore.case='true')])
varlist=varlist[  !(varlist %in% varlist[grep(paste(c("^al_n2phi","^al_netw30_rc"),collapse="|"),varlist,ignore.case='true')])]
IsChar=c(rep(0,length(varlist)))
Variable_Category=c('Response',rep('Predictor',length(varlist)-1))
                    
selectionlist=data.frame(Attribute=varlist,Variable_Category ,IsChar)
setwd('R:/EmployerGroup/Analytics/Two Plus NB Morbidity Modeling/RPrograms')

write.table(selectionlist,file="groupmodel.csv",quote=FALSE,row.names=FALSE,col.names=TRUE,  sep=" ")

model0=get_formula(attr_list_csv_file = "groupmodel.csv",version_name = "fullmodel")
model1=get_formula(attr_list_csv_file = "groupmodel.csv",version_name = "alnombr")

mbrwgt=(tdata$mbrs/sum(tdata$mbrs))*length(tdata$mbrs)

############################################################################################################################################


cvglmnet0 <-cv.glmnet.formula(formula=model0,data=tdata,weights=mbrwgt,
                              family=c("poisson"), 
                              alpha = 1)


selected_cv_coefs0 <- coef(cvglmnet0,s="lambda.1se")
selected_cv_coefs0out=selected_cv_coefs0[selected_cv_coefs0[,1]!=0,1]
cvtscore = predict(cvglmnet0,tdata, s="lambda.1se")
cvvscore = predict(cvglmnet0, vdata,s="lambda.1se")
cvtscore=ifelse(is.na(cvtscore),0,cvtscore)
cvvscore=ifelse(is.na(cvvscore),0,cvvscore)
predictionout0=  c(cvtscore,cvvscore)
#vdata = vdata[is.na(vdata$manual_claims_pmpm)==0,]
cov.wt(cbind(vdata$manual_claims_pmpm,vdata$agnostic_claims_pmpm),wt=vdata$mbrs,cor=TRUE)$cor[1,2]
cov.wt(cbind(vdata$manual_claims_pmpm*exp(cvvscore),vdata$agnostic_claims_pmpm),wt=vdata$mbrs,cor=TRUE)$cor[1,2]
#0.428425
#0.433782
#standardize coefficients to evaluate variable importance
preddata=tdata[,grep(paste(names(selected_cv_coefs0out),collapse="|"),colnames(tdata),ignore.case='true')]

sds=apply(preddata,2,sd)
sds=sds[names(sds) %in% (names(selected_cv_coefs0out))]
sds =sds[order(names(sds))]
as.matrix(sds)

coefs = (as.matrix(selected_cv_coefs0out)[-1,])
coefs =coefs[order(names(coefs))]
std_coefs= coefs*sds
std_coefs = std_coefs[order(-std_coefs)]
as.matrix(std_coefs)

############################################################################################################################################

cvglmnet1 <-cv.glmnet.formula(formula=model1,data=tdata,weights=mbrwgt,
                              family=c("poisson"), 
                              alpha = 1)


selected_cv_coefs1 <- coef(cvglmnet1,s="lambda.1se")
selected_cv_coefs1out=selected_cv_coefs1[selected_cv_coefs1[,1]!=0,1]
cvtscore = predict(cvglmnet1,tdata, s="lambda.1se")
cvvscore = predict(cvglmnet1, vdata,s="lambda.1se")
cvtscore=ifelse(is.na(cvtscore),0,cvtscore)
cvvscore=ifelse(is.na(cvvscore),0,cvvscore)
predictionout1=  c(cvtscore,cvvscore)
#vdata = vdata[is.na(vdata$manual_claims_pmpm)==0,]
cov.wt(cbind(vdata$manual_claims_pmpm,vdata$agnostic_claims_pmpm),wt=vdata$mbrs,cor=TRUE)$cor[1,2]
cov.wt(cbind(vdata$manual_claims_pmpm*exp(cvvscore),vdata$agnostic_claims_pmpm),wt=vdata$mbrs,cor=TRUE)$cor[1,2]
#0.428425
#0.433782
#standardize coefficients to evaluate variable importance
preddata=tdata[,grep(paste(names(selected_cv_coefs1out),collapse="|"),colnames(tdata),ignore.case='true')]

sds=apply(preddata,2,sd)
sds=sds[names(sds) %in% (names(selected_cv_coefs1out))]
sds =sds[order(names(sds))]
as.matrix(sds)

coefs = (as.matrix(selected_cv_coefs1out)[-1,])
coefs =coefs[order(names(coefs))]
std_coefs= coefs*sds
std_coefs = std_coefs[order(-std_coefs)]
as.matrix(std_coefs)


############################################################################################################################################
############################################################################################################################################
############################################################################################################################################


tout= with(tdata,data_frame(SRC_RPT_CUST_ID,lastrenwldt_cln,partition,manual_claims_pmpm,mbrs))
vout= with(vdata,data_frame(SRC_RPT_CUST_ID,lastrenwldt_cln,partition,manual_claims_pmpm,mbrs))

foroutput= mutate(rbind(tout,vout),cvlasso1sepred=predictionout0 ,cvlassonombrpred=predictionout1)

write.table(foroutput,file="groupcvpreds.csv",quote=FALSE,row.names=FALSE,col.names=TRUE,  sep=" ")

write.table(rbind(as.matrix(selected_cv_coefs0out),as.matrix(selected_cv_coefs1out)),file="groupcvmodel.csv",quote=FALSE,row.names=TRUE,col.names=FALSE,  sep=" ")









############################################################################################################################################

vout= with(vdata,data_frame(SRC_RPT_CUST_ID,lastrenwldt_cln,partition,agnostic_claims_pmpm,manual_claims_pmpm))
tout= with(tdata,data_frame(SRC_RPT_CUST_ID,lastrenwldt_cln,partition,agnostic_claims_pmpm,manual_claims_pmpm))
foroutput= mutate(rbind(vout,tout),lassopred=predictionout1,testpred=exp(predictionout1)*manual_claims_pmpm)

write.table(foroutput,file="grouppreds.csv",quote=FALSE,row.names=FALSE,col.names=TRUE,  sep=" ")

write.table(selected_step_coefs1out,file="grouplassomodel.csv",quote=FALSE,row.names=FALSE,col.names=TRUE,  sep=" ")
############################################################################################################################################
############################################################################################################################################
############################################################################################################################################





