library(tidyverse)
library(data.table)
library(xlsx)
library(readxl)

default_dir <- "J:/deans/Presidents/SixSigma/MSHS Productivity/Productivity/Volume - Data/MSBI Data/Charge Detail/Calculation Worksheets"

begin_date <- as.Date("2021/08/01")
final_date <- as.Date("2021/08/28")


month_dir <- choose.dir(default = default_dir)

# read file path
all_folders <-
  list.dirs(
    path = month_dir,
    full.names = TRUE
  )

all_paths <-
  list.files(
    path = all_folders,
    pattern = "*.txt",
    full.names = TRUE
  )

# read file content
all_content <-
  all_paths %>%
  lapply(read.table,
         header = TRUE,
         sep = "|",
         encoding = "UTF-8",
         stringsAsFactors = F
  )

# read file name
all_filenames <- all_paths %>%
  basename() %>%
  as.list()

# combine file content list and file name list
all_lists <- mapply(c, all_content, all_filenames, SIMPLIFY = FALSE)

# unlist all lists and change column name
all_result <- rbindlist(all_lists, fill = T)

rm(all_lists, all_content)

# change column name
names(all_result)[10] <- "File.Path"
saveRDS(all_result, file = paste0(month_dir, "/all_merged.rds"))
write.table(all_result, file = paste0(month_dir, "/all_merged.csv"),
            row.names = F, col.names = T, sep = ",")

# all_result <- readRDS(file = paste0(month_dir, "/all_merged.rds"))
all_result$TransDate <- as.Date(all_result$TransDate, format = "%m/%d/%Y")
all_result$ChargeCode <- stringr::str_trim(all_result$ChargeCode)
# all_result <- all_result %>%
#   filter(EntityId == "MSBI" | EntityId == "MSCCW")

map_file_folder <- "J:/deans/Presidents/SixSigma/MSHS Productivity/Productivity/Volume - Data/MSBI Data/Charge Detail/Instruction Files/*.*"

path_dictionary_pay_cycles <- choose.files(default = map_file_folder,
                                           caption = "Select Pay Cycles File",
                                           multi = F)
dictionary_pay_cycles <- read_xlsx(path_dictionary_pay_cycles,
                                   sheet = 1,
                                   col_types = c("date", "skip", "skip", "skip",
                                                 "skip", "skip", "skip", "skip",
                                                 "skip", "skip", "date", "date",
                                                 "skip"))
dictionary_pay_cycles$Date <- as.Date(dictionary_pay_cycles$Date)
dictionary_pay_cycles$`Start Date` <- as.Date(dictionary_pay_cycles$`Start Date`)
dictionary_pay_cycles$`End Date` <- as.Date(dictionary_pay_cycles$`End Date`)

CDM_file_path <- choose.files(default = map_file_folder,
                              caption = "Select CDM File",
                              multi = F)
CDM <- read_xlsx(CDM_file_path, sheet = 1) # , col_types = c('date', 'skip', 'skip', 'skip', 'skip','skip', 'skip','skip', 'skip','skip','date', 'date', 'skip'))
CDM_slim <- CDM %>% select(CHARGE_CODE, CHARGE_DESC, OPTB_cpt4)
CDM_slim$CHARGE_CODE <- stringr::str_trim(CDM_slim$CHARGE_CODE)
CDM_slim$OPTB_cpt4[is.na(CDM_slim$OPTB_cpt4)] <- "0"

CC_xwalk_file_path <- choose.files(default = map_file_folder,
                                   caption = "Select Crosswalk File", multi = F)
CC_xwalk <- read_xlsx(CC_xwalk_file_path, sheet = 1) # , col_types = c('date', 'skip', 'skip', 'skip', 'skip','skip', 'skip','skip', 'skip','skip','date', 'date', 'skip'))
CC_xwalk$`EPSI Revenue Department` <- as.character(CC_xwalk$`EPSI Revenue Department`)


all_result <- left_join(x = all_result,
                        y = dictionary_pay_cycles,
                        by = c("TransDate" = "Date"),
                        all.x = T)

all_result <- left_join(x = all_result,
                        y = CC_xwalk,
                        by = c("FacilityId" = "FacilityId",
                               "RevDept" = "EPSI Revenue Department"),
                        all.x = T)

all_result <- left_join(x = all_result,
                        y = select(CDM_slim, -any_of("CHARGE_DESC")),
                        by = c("ChargeCode" = "CHARGE_CODE"),
                        all.x = T)

all_result$OPTB_cpt4[is.na(all_result$OPTB_cpt4)] <- "#N/A"

NA_CC_result <- all_result %>%
  filter(is.na(`Labor Department`) |
           str_detect(`Labor Department`, "NOMAP")) %>%
  group_by(FacilityId, RevDept) %>%
  summarise(Vol = sum(Qty))

NA_CPT4_result <- all_result %>%
  filter(OPTB_cpt4 == "#N/A" | OPTB_cpt4 == 0 |
           OPTB_cpt4 == 99999 | is.na(OPTB_cpt4)) %>%
  group_by(FacilityId, RevDept, ChargeCode, OPTB_cpt4) %>%
  summarise(Vol = sum(Qty))

charge_summary <- all_result %>%
  group_by(`Labor Department`, TransDate, OPTB_cpt4) %>%
  summarise(Vol = sum(Qty))

charge_summary <- charge_summary %>%
  filter(TransDate >= begin_date &
           TransDate <= final_date)

charge_summary$EntityID <- rep(729805, nrow(charge_summary))
charge_summary$FacilID <- rep(630571, nrow(charge_summary))
charge_summary$budget <- rep(0, nrow(charge_summary))
charge_summary <- charge_summary %>%
  mutate(EndDate = TransDate) %>%
  rename(StartDate = TransDate) %>%
  select(EntityID, FacilID, `Labor Department`, StartDate, EndDate,
         OPTB_cpt4, Vol, budget) %>%
  filter(!is.na(`Labor Department`)) %>%
  filter(!is.na(OPTB_cpt4)) %>%
  filter(OPTB_cpt4 != "#N/A") %>%
  filter(OPTB_cpt4 != 0) %>%
  filter(OPTB_cpt4 != 99999) %>%
  filter(!str_detect(`Labor Department`, "NOMAP"))

charge_summary$StartDate <- format(charge_summary$StartDate, "%m/%d/%Y")
charge_summary$EndDate <- format(charge_summary$EndDate, "%m/%d/%Y")

# --------------------------------------------

write.table(charge_summary, file = paste0(month_dir,"/CPT upload.csv"),
            row.names = F, col.names = F, sep = ",")
write.table(NA_CC_result, file = paste0(month_dir, "/CPT no CC.csv"),
            row.names = F, col.names = T, sep = ",")
write.table(NA_CPT4_result, file = paste0(month_dir, "/CPT no CPT4 map.csv"),
            row.names = F, col.names = T, sep = ",")

