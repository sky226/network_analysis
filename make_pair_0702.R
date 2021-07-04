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
D<-read.csv("D:/RESULTS/network/database/under65/2011/db_under65_2011_4.csv")
str(D)
names(D)<-c('KEY_SEQ','GNL_NM_CD')

#########################################################################
#####################step 0:  group by KEY_SEQ
#########################################################################
unique_key<-length(unique(D$KEY_SEQ))
groupby<-split(D$GNL_NM_CD, D$KEY_SEQ)
rm(D)
save(groupby, file="D:/RESULTS/network/database/under65/2011/grooupby_2011.RData")

#########################################################################
#####################step 1: multiple processing: combination and rbind 
#########################################################################
ncore<-parallel::detectCores()-1
#cores<-memory.limit()/ memory.size()
combind.parallel <- function(list,ncore)
{
  library(parallel)
  cl<-makeCluster(ncore,outfile="D:/RESULTS/network/database/under65/2011/log.txt")
  parallel::clusterEvalQ(cl, c(library(RcppAlgos), library(base)))
  combine.parallel <-function(x){comboGeneral(x,2,repetition=FALSE,
                                              Parallel=TRUE,nThreads= ncore )}
  list.join<-parLapply(cl,list,combine.parallel)
  stopCluster(cl)
  list.out<-do.call(rbind,list.join)
  return(list.out)
}
comb <-combind.parallel(groupby,11)
rm(groupby)
write.csv(comb, "D:/RESULTS/network/database/under65/2011/comb.csv")

#########################################################################
#####################step 2 : unique list 
#########################################################################
library(dplyr)
unique<- funique(comb)
write.csv(unique, "D:/RESULTS/network/database/under65/2011/unique.csv")
##convert data.frame
unique.df<-as.data.frame(unique)
#rm(unique)
#head(unique.df)
unique.df$V3 <-unique.df$V1-unique.df$V2   #213471 rows
head(unique.df)

p<-subset(unique.df, unique.df$V3>0)
n<-subset(unique.df, unique.df$V3<0)
p<-p[,c(1,2)]
n<-data.frame(V1= n$V2, V2=n$V1)
head(n)
unique.df<-unique(bind_rows(p,n)) ## 119417

write.csv(unique.df,"D:/RESULTS/network/database/under65/2011/unique.csv")
