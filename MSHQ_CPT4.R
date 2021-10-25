#MSHQ Charges
library(dplyr)
library(tidyr)
library(readxl)
library(lubridate)
library(xlsx)

#Bring in Rev Dep to CC crosswalk
rev_map <- read_excel("J:\\deans\\Presidents\\SixSigma\\MSHS Productivity\\Productivity\\Volume - Data\\MSH Data\\Charges\\MSHQ REV CROSSWALK.xlsx") %>%
  select(c(1,5:8)) %>%
  distinct()
#Bring in CPT reference table
#For months: January,April,July,October there is a new cpt_ref to download
cpt_ref <- read_excel("J:\\deans\\Presidents\\SixSigma\\MSHS Productivity\\Productivity\\Volume - Data\\MSH Data\\Charges\\CPT Reference\\CPT_Ref.xlsx") %>%
  select(1,2,3,5,11,12)

#Prepares file for master
charges <- function(MSH,MSQ){
  #colnames for the charge details
  chargenames <- c("SINAI.CODE","REV.DEP","DESCRIPTION","CPT","QTY","MONTH")
  colnames(MSH) <- chargenames
  colnames(MSQ) <- chargenames
  #combine MSH and MSQ charge details
  MSHQ <- rbind(MSH,MSQ)
  #remove blank CPT lines
  MSHQ <- filter(MSHQ,!is.na(CPT))
  #establish what quarter of the year it is
  if(max(MSHQ$MONTH) < 4){
    Q <<- 1
  } else if(max(MSHQ$MONTH) < 7){
    Q <<- 2
  } else if(max(MSHQ$MONTH) < 10){
    Q <<- 3
  } else {
    Q <<- 4
  }
  #bring in revenue center mapping
  MSHQ <- left_join(MSHQ,rev_map) %>% 
    #remove non-premier revenue centers
    filter(!is.na(ORACLE.CC)) %>%
    #begin formatting for upload
    mutate(PARTNER = "729805",
           BUDGET = "0",
           HOSP = "NY0014",
           START = as.Date(paste0(MONTH,"/1/",Year),format = "%m/%d/%Y"),
           QUARTER = Q,
           QTY = as.numeric(QTY))
  #Add start and end date and finish formatting
  MSHQ <- mutate(MSHQ,
                 END = paste0(substr(START,6,7),"/",days_in_month(START),"/",Year),
                 START = paste0(substr(START,6,7),"/",substr(START,9,10),"/",Year),
                 REP.DEFINITION = paste0(REP.ID," - ",REP.NAME)) %>%
    group_by(PARTNER,HOSP,ORACLE.CC,START,END,CPT,BUDGET,CPT.GROUP,REP.DEFINITION,QUARTER) %>%
    summarise(QTY = sum(QTY, na.rm=T)) %>%
    select(PARTNER,HOSP,ORACLE.CC,START,END,CPT,QTY,BUDGET,CPT.GROUP,REP.DEFINITION,QUARTER)
  return(MSHQ)
}
#Creates master repository and master trend
master <- function(){
  #read in master cpt file
  master <- readRDS("J:\\deans\\Presidents\\SixSigma\\MSHS Productivity\\Productivity\\Volume - Data\\MSH Data\\Charges\\Master\\master.RDS")
  #Check that new charge detail does not overlap with master
  if(max(as.Date(master$END,format = "%m/%d/%Y")) < min(as.Date(MSHQ$START,format = "%m/%d/%Y"))){
    master <- rbind.data.frame(master,MSHQ)
  } else {
    stop("Raw data overlaps with master")
  }
  #trend out the master file by month to validate data
  master_trend <- master %>%
    mutate(`Concatenate for lookup` = paste0(substr(END,7,10),"Q",QUARTER,CPT)) %>%
    left_join(.,cpt_ref) %>%
    mutate(LABOR = case_when(
      CPT.GROUP == "PROCEDURE" ~ QTY*`CPT Procedure Count`,
      CPT.GROUP == "LAB" ~ QTY*`Lab Procedure Count`,
      CPT.GROUP == "RVU" ~ QTY*`Facility Total RVU Factor`),
      LABOR = as.numeric(LABOR),
      DATE = as.Date(END, format = "%m/%d/%Y")) %>%
    filter(LABOR > 0) %>%
    group_by(REP.DEFINITION,CPT.GROUP,END,DATE)%>%
    summarise(LABOR = sum(LABOR,na.rm = T)) %>%
    arrange(DATE) %>%
    pivot_wider(id_cols = c(REP.DEFINITION,CPT.GROUP),names_from = END,values_from = LABOR)
  #save master and trend to global environment
  master <<- master
  master_trend <<- master_trend
}
#Saves master files and upload
upload_master <- function(){
  saveRDS(master,"J:\\deans\\Presidents\\SixSigma\\MSHS Productivity\\Productivity\\Volume - Data\\MSH Data\\Charges\\Master\\master.rds")
  write.xlsx(as.data.frame(master_trend),"J:\\deans\\Presidents\\SixSigma\\MSHS Productivity\\Productivity\\Volume - Data\\MSH Data\\Charges\\Master\\master_trend.xlsx",
             row.names = F)
  upload <- MSHQ %>% ungroup() %>% select(c(1:8))
  upload <<- upload
  start <- as.Date(min(MSHQ$START),format = "%m/%d/%Y")
  end <- as.Date(max(MSHQ$END),format = "%m/%d/%Y")
  smonth <- month(start)
  smonth <- toupper(month.abb[month(smonth)])
  emonth <- smonth
  sday <- format(as.Date(start, format="%Y-%m-%d"), format="%d")
  eday <- format(as.Date(end, format="%Y-%m-%d"), format="%d")
  syear <- substr(start, start=1, stop=4)
  eyear <- substr(end, start=1, stop=4)
  name <- paste0("MSHQ_CPT4_",sday,smonth,syear," to ",eday,emonth,eyear,".csv")
  upload_path <- paste0("J:\\deans\\Presidents\\SixSigma\\MSHS Productivity\\Productivity\\Volume - Data\\MSH Data\\Charges\\Uploads\\",name)
  write.table(upload,upload_path,sep = ",",row.names = F,col.names = F)
}

#Bring in all sheets in charges file
path <- file.choose()
sheetnames <- excel_sheets(path)
mylist <- lapply(excel_sheets(path), read_excel, path = path, col_names = F)
names(mylist) <- sheetnames
names(mylist)

#Enter Year of data
Year <- "2021"
#Execute functions
MSHQ <- charges(MSH = mylist[[1]],MSQ = mylist[[3]])
#Create master and master trend
master()
#Review master trend

#Create upload and save both master files and upload
upload_master()

