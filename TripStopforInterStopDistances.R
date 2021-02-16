library(readxl)
library(data.table)
library(dplyr)
library(tidyr)

#write.xlsx and write.xlsx2 doesn't work here, tripstop files are too large for the java cache to handle

#Pulling in all excel data from this folder
setwd("G:/OPER_ADM/Service Evaluation/Scheduled Information/TripStop_Information/Processing For InterStopDistance and Time/Drop Raw Files Here")
file.list <- list.files(pattern = '*.xlsx')
df.list <- lapply(file.list, function(x) read_excel(x, col_types = "text"))
setwd("~/")

#Combining all excel documents into 1 big data frame
attr(df.list, "names") <- file.list
CombinedDF = rbindlist(df.list, idcol="id")

CombinedDF$Stop <- as.double(CombinedDF$Stop)
CombinedDF$tstp_pass_time_min <- as.numeric(CombinedDF$tstp_pass_time_min)
CombinedDF$stop_rank <- as.numeric(CombinedDF$stop_rank)
CombinedDF$trp_number_trim <- as.numeric(CombinedDF$trp_number_trim)
CombinedDF$Distance <- as.numeric(CombinedDF$Distance)

#Logic to identify all May/June months as 'S' and all other months as 'O' in preparation for exception logic
CombinedDF$SummerSensor <- ifelse(grepl("06_TripStop.xlsx", CombinedDF$id), "S", ifelse(grepl("05_TripStop.xlsx", CombinedDF$id), "S", "O"))
CombinedDF$EnhancedSensor <- paste(CombinedDF$SummerSensor, CombinedDF$`Evt, Sta`, sep = ",")

# Olist <- c("O,AZoff", "O,PHoff", "O,C2on", "O,FBoff", "O,FBon", "O,RDoff", "O,RToff", "O,SToff", "O,REoff", "O,RIoff", "O,RYoff", "O,RKoff", "O,RJoff", "O,SJoff", "O,RVoff", "O,RNoff", "O,RQoff", "O,RPoff", "O,RRoff", "O,RSoff", "O,UMoff", "O,RUoff", "O,SUoff", "O,RCoff", "O,RFoff", "O,SFoff", "O,RAoff", "O,SAoff", "O,RLoff", "O,ROoff", "O,RXoff", "O,RGoff", "O,SGoff", "O,LSon")
# 
# Slist <- c("S,AZoff", "S,PHoff", "S,C2on", "S,FBoff", "S,FBon", "S,RDon", "S,RTon", "S,STon", "S,REon", "S,RIon", "S,RYon", "S,RKon", "S,RJon", "S,SJon", "S,RVon", "S,RNon", "S,RQon", "S,RPon", "S,RRon", "S,RSon", "S,UMon", "S,RUon", "S,SUon", "S,RCon", "S,RFon", "S,SFon", "S,RAon", "S,SAon", "S,RLon", "S,ROon", "S,RXon", "S,RGon", "S,SGon", "S,LSon")
# 
# CombinedDF <- CombinedDF %>%
#   filter(!CombinedDF$EnhancedSensor %in% Olist)
# 
# CombinedDF <- CombinedDF %>%
#   filter(!CombinedDF$EnhancedSensor %in% Slist)

#Removal of all car trips
# CombinedDF <- CombinedDF %>%
#   filter(CombinedDF$Route != "Car")

#Looking at filename to seperate into M-F, S, A, and HOL schedules
CombinedDF$`Service Day` <- ifelse(grepl("_WK.xlsx", CombinedDF$id), "M-F", ifelse(grepl("_SA.xlsx", CombinedDF$id), "SAT", ifelse(grepl("_SU.xlsx", CombinedDF$id), "SUN", "HOL")))


# #Looking at Op Day to seperate into M-F, S, A, and HOL schedules (This logic will tag all schedules that are not identified as Sunday, Saturday or Holiday as M-F)
# CombinedDF$`Service Day` <- ifelse(grepl("smuwtfa", CombinedDF$`Op Day`), "HOL", ifelse(grepl("s", CombinedDF$`Op Day`), "SUN", ifelse(grepl("a", CombinedDF$`Op Day`), "SAT", "M-F")))

# #Removal of trips that do not conform to M-F
# CombinedDF <- CombinedDF %>%
#   filter(CombinedDF$`Service Day` != "null")

CombinedDF <- CombinedDF %>%
  arrange(id, trp_number_trim, trp_oper_day, `Op Day`, `Service Period`, Route, Direction, Block, stop_rank) %>%
  group_by(id, trp_number_trim, trp_oper_day, `Op Day`, `Service Period`, Route, Direction, Block, `Evt, Sta`) %>%
  mutate(PreviousMinutes = lag(tstp_pass_time_min))

CombinedDF$TravelTime <- CombinedDF$tstp_pass_time_min - CombinedDF$PreviousMinutes

CombinedDF$TravelTime[is.na(CombinedDF$TravelTime)] <- 0
CombinedDF$lay_time_tenths[is.na(CombinedDF$lay_time_tenths)] <- 0
CombinedDF$Place[is.na(CombinedDF$Place)] <- ""
CombinedDF$`Evt, Sta`[is.na(CombinedDF$`Evt, Sta`)] <- ""
CombinedDF$Route[is.na(CombinedDF$Route)] <- "Deadhead"
CombinedDF$Direction[is.na(CombinedDF$Direction)] <- "Deadhead"

CombinedDF$InterStpTime <- (CombinedDF$TravelTime/60)
CombinedDF$InterStp_Dist <- (CombinedDF$Distance/5280)
CombinedDF$Trp_time <- CombinedDF$Time05
CombinedDF$Ttl_layover <- CombinedDF$lay_time_tenths
CombinedDF$Ttl_time <- (as.numeric(CombinedDF$Trp_time) + as.numeric(CombinedDF$Ttl_layover))
CombinedDF$Block <- gsub('\\s+', '', CombinedDF$Block)

TrimmedDF <- CombinedDF %>%
  ungroup %>%
  select(trp_number_trim, `Op Day`, `Service Period`, Route, Direction, Block, Place, Stop, stop_rank, `Stop Name`, tstp_pass_time_24h1, InterStpTime, Ttl_time, InterStp_Dist, Distance1, `Evt, Sta`, `Service Day`, Trp_time, Ttl_layover)

TrimmedDFHOL <- TrimmedDF %>%
  filter(`Service Day` == "HOL") %>%
  arrange(trp_number_trim)

TrimmedDFMF <- TrimmedDF %>%
  filter(`Service Day` == "M-F") %>%
  arrange(trp_number_trim)

TrimmedDFSAT <- TrimmedDF %>%
  filter(`Service Day` == "SAT") %>%
  arrange(trp_number_trim)

TrimmedDFSUN <- TrimmedDF %>%
  filter(`Service Day` == "SUN") %>%
  arrange(trp_number_trim)

#This works, however, there are trips that do not match with the Distance1
ExperimentalTrim <- TrimmedDFMF %>%
  group_by(trp_number_trim, Route, `Service Period`, `Service Day`, `Block`, Distance1) %>%
  summarise_at(vars(InterStp_Dist, InterStpTime), sum)

write.csv(TrimmedDFHOL, "G:/OPER_ADM/Service Evaluation/Scheduled Information/TripStop_Information/Processing For InterStopDistance and Time/Take output CSV Here/OutputHOL.csv")

write.csv(TrimmedDFMF, "G:/OPER_ADM/Service Evaluation/Scheduled Information/TripStop_Information/Processing For InterStopDistance and Time/Take output CSV Here/OutputMF.csv")

write.csv(TrimmedDFSAT, "G:/OPER_ADM/Service Evaluation/Scheduled Information/TripStop_Information/Processing For InterStopDistance and Time/Take output CSV Here/OutputSAT.csv")

write.csv(TrimmedDFSUN, "G:/OPER_ADM/Service Evaluation/Scheduled Information/TripStop_Information/Processing For InterStopDistance and Time/Take output CSV Here/OutputSUN.csv")

write.csv(TrimmedDF, "G:/OPER_ADM/Service Evaluation/Scheduled Information/TripStop_Information/Processing For InterStopDistance and Time/Take output CSV Here/OutputCombo.csv")
