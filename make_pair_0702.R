install.packages("Rcpp")
install.packages("RcppAlgos")
install.packages("purrr")
install.packages("data.table")
install.packages("plyr")
install.packages("dplyr")
install.packages("foreach")
install.packages("doParallel")
if(!require(parallel)) {
  install.packages("parallel")
}
install.packages("rlist")
install.packages("funique")
library(rlist)
library(Rcpp)
library(RcppAlgos)
library(purrr)
library(data.table)
library(reshape)
library(plyr)
library(dplyr)
library(foreach)
library(doParallel)
library(parallel)
library(utils)
library(funique)
########################################################################
####################################data read 
########################################################################
rm(list=ls())
gc()
D<-read.csv("C:/network/results/hypertension/2011/db_hypertension_2011.csv")
str(D)
names(D)<-c('KEY_SEQ','GNL_NM_CD')

#########################################################################
#####################step 0:  group by KEY_SEQ
#########################################################################
unique_key<-length(unique(D$KEY_SEQ))
groupby<-split(D$GNL_NM_CD, D$KEY_SEQ)
rm(D)
save(groupby, file="C:/network/results/hypertension/2011/grooupby_hypertension_2011.RData")
#########################remove length =1 list 

load("C:/network/results/hypertension/2011/grooupby_hypertension_2011.RData")
str(groupby)
#remove length ==1 
id <-sapply(groupby, function(x) length(x)>1)
class(groupby)
groupby2<-groupby[id]
rm(groupby,id)
# n<-length(groupby2)/2
# group1<-groupby2[1:n]
# group2<-groupby2[(n+1):(2*n)]
#########################################################################
#####################step 1: multiple processing: combination and rbind 
#########################################################################

ncore<-parallel::detectCores()-1
#cores<-memory.limit()/ memory.size()
combind.parallel <- function(list,ncore)
{
  library(parallel)
  cl<-parallel::makeCluster(ncore,type="PSOCK",outfile="C:/network/results/hypertension/2011/log.txt")
  ##error½Ã ncore´ë½Å 120
  parallel::clusterEvalQ(cl, c(library(RcppAlgos), library(base)))
  combine.parallel <-function(x){comboGeneral(x,2,repetition=FALSE,
                                              Parallel=TRUE,nThreads= ncore )}
  list.join<-parLapply(cl,list,combine.parallel)
  stopCluster(cl)
  list.out<-do.call(rbind,list.join)
  return(list.out)
}
comb <-combind.parallel(groupby2,5)
# comb1 <-combind.parallel(group1,5)
# comb2 <-combind.parallel(group2,5)
# comb<-rbind(comb1, comb2)
rm(groupby2)
write.csv(comb, "C:/network/results/hypertension/2011/comb.csv")

#########################################################################
#####################step 2 : order by rules
#########################################################################
##convert data.frame
df<-as.data.frame(comb)
str(df)
mode(df$V1)<-"numeric"
mode(df$V2)<-"numeric"
df<-na.omit(df)
sum(is.na(df))
## order by rules
df$V3 <- df$V1-df$V2   #213471 rows
head(df)
p<-subset(df, df$V3>0)
n<-subset(df, df$V3<0)
p<-p[,c(1,2)]
n<-data.frame(V1= n$V2, V2=n$V1)
head(n)
df<-bind_rows(p,n) ## 119417
#########################################################################
#####################step 3 : count frequency
#########################################################################
str(df)
rm(comb)
counts<-ddply(df, .(df$V1, df$V2), nrow)
names(counts)<-c("drug1","drug2","Freq")
str(counts)
rm(n,p)
write.csv(counts,"C:/network/results/hypertension/2011/counts.csv")

