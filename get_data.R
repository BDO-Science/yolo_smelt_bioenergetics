################################################
# Download USGS and DWR Temperature and Turbidity Data in Yolo
# Cat Pien, USBR cpien@usbr.gov
# Last updated 8/29/2023
#################################################

library(dataRetrieval)
library(janitor)
library(lubridate)
library(here)
library(tidyr)
library(dplyr)
library(sharpshootR)
library(ggplot2)
library(patchwork)

# Functions ---------------------------------------------------------------

## NWIS ----------
f_get_NWIS_data <- function(siteNumbers="11455140", parameterCd = c('63680'), startDate = start, endDate = end, tz = "Etc/GMT+8"){

  # get data
  print("Downloading data...")
  data <- dataRetrieval::readNWISuv(siteNumbers, parameterCd, startDate, endDate)

  # fix names

  data2 <- dataRetrieval::renameNWISColumns(data)

  print("Data downloaded!")

  # clean names
  data2 <- janitor::clean_names(data2)

  # write out
  saveRDS(data2, paste0("data_raw/USGS_NWIS_", siteNumbers, "_turb_temp.rds"), compress = "xz")

  # print message!
  print("Data saved in data_raw")
}

# USGS Data ------------------------------
## Download TOE DRAIN @ Liberty Island near Courtland CA -----
site_id <- "11455140"
startDate = lubridate::ymd("2014-09-16", tz = "America/Los_Angeles")
endDate = lubridate::ymd("2023-01-01", tz = "America/Los_Angeles")

# Metadata https://waterdata.usgs.gov/monitoring-location/11455140/#parameterCode=00065&period=P7D
# Temperature = 00010 (BGC starts 2017-05-23),
# Turbidity = 63689 (TS213:2014-09-16 to 2016-10-13/BGC: 2017-05-23)
# There are two turbidity sources that I combine below
f_get_NWIS_data(siteNumbers = site_id, parameterCd = c("63680", "00010"),
                startDate = startDate, endDate = endDate)

## Clean up data -------------
data_TOE <- readRDS(here("data_raw", "USGS_NWIS_11455140_turb_temp.rds")) %>%
  rename(Datetime = date_time,
         turb = ts213_ysi_exo_turb_inst,
         turb2 = bgc_project_turb_inst,
         temp = wtemp_inst)%>%
  mutate(station = "TOE",
         date = date(Datetime),
         datetime = Datetime)

temp_TOE <- data_TOE %>%
  select(datetime, date, temp_TOE= temp)
turb_TOE1 <- data_TOE %>%
  select(datetime, date, turb) %>%
  filter(!is.na(turb))
turb_TOE2 <- data_TOE %>%
  select(datetime, date, turb2)%>%
  filter(!is.na(turb2)) %>%
  rename(turb = turb2)
turb_TOE <- rbind(turb_TOE1, turb_TOE2) %>%
  rename(turb_TOE = turb)
turb_TOE_check <- turb_TOE %>%
  group_by(date) %>%
  summarize(n = n())
data_TOE_all <- full_join(turb_TOE, temp_TOE)

## Option for long ------------------------------------------------
data_TOE_long <- data_TOE_all %>%
  pivot_longer(cols = c(turb,temp),
               names_to = "parameter",
               values_to = "value")
## Write data -------------------------------------------------------
# saveRDS(data_TOE_all, here("data_clean", "USGS_11455140_turb_temp_2014_2023.rds"), compress = "xz")

# LIS data ---------------------------------------------
## Download data ----------------------

# Metadata https://cdec.water.ca.gov/dynamicapp/staMeta?station_id=LIS
# Temperature = 25 (starts 2008-07-16)
# Turbidity = 221 (starts 2013-08-01)

sensors = c(221, 25)
data_LIS <- lapply(sensors,
                function(x){
                  CDECquery(id = "LIS", sensor = x, interval = "E", start = "2013-08-01",
                            end = "2023-01-01")})

## Clean up data ----------------------
data_LIS_all <- bind_rows(data_LIS)%>%
  filter(!is.na(datetime)) %>%
  mutate(sensor = case_when(sensor_num == 221 ~ "turb_LIS",
                            sensor_num == 25 ~ "temp_LIS"),
         sensor = as.character(sensor)) %>%
  mutate(date = date(datetime)) %>%
  select(datetime, date, sensor, value) %>%
  pivot_wider(names_from= "sensor",
              values_from = value,
              id_cols = c("datetime", "date")) %>%
  mutate(temp_LIS = (temp_LIS - 32)*5/9) %>%
  filter(temp_LIS <50 & temp_LIS > 0) %>%
  filter(turb_LIS >0 & turb_LIS <1000)

LIS_summary <- LIS_df2 %>%
  group_by(datetime) %>%
  summarize(n = n())

## Write data ------------------------------------
# saveRDS(data_LIS_all, here("data_clean", "CDEC_LIS_turb_temp_2013_2023.rds"), compress = "xz")


# Combine LIS and TOE data -------------------------------

temp_turb_data <- full_join(data_LIS_all, data_TOE_all)
data_complete <- na.omit(temp_turb_data)

saveRDS(data_complete, here("data_clean", "LIS_TOE_complete_turb_temp.rds"))
saveRDS(temp_turb_data, here("data_clean", "LIS_TOE_fulljoin_turb_temp.rds"), compress = "xz")

# Plot data ---------------------------------------
plot_turbidity <- ggplot(data_complete) + geom_point(aes(turb_LIS, turb_TOE)) + labs(title = "15 minute turbidity") + theme_bw()
plot_temp <- ggplot(data_complete) + labs(title = "15 minute temperature") + geom_point(aes(temp_LIS, temp_TOE)) +
  geom_smooth(aes(temp_LIS, temp_TOE)) + theme_bw()

png(here("output", "turb_plot_event.png"),width = 8, height = 8, units = "in", res = 300)
plot_turbidity
dev.off()

png(here("output", "temp_plot_event.png"),width = 8, height = 8, units = "in", res = 300)
plot_temp
dev.off()

# Convert to daily data --------------------------------------
temp_turb_data_daily <- temp_turb_data %>%
  group_by(date) %>%
  summarize(temp_LIS = mean(temp_LIS, na.rm = TRUE),
            temp_TOE = mean(temp_TOE, na.rm = TRUE),
            turb_LIS = mean(turb_LIS, na.rm = TRUE),
            turb_TOE = mean(turb_TOE, na.rm = TRUE))
data_daily_complete <- na.omit(temp_turb_data_daily)

# Plot daily data  ------------------------------------

## temp -------
(temp_daily_corr <- ggplot(data_daily_complete) + geom_point(aes(temp_LIS, temp_TOE)) + theme_bw())

(temp_timeseries <- ggplot(data_daily_complete) +
  geom_point(aes(date, temp_LIS), color = "steelblue4") +
  geom_point(aes(date, temp_TOE), color = "goldenrod") +
  theme_bw())

(temp_plots <- temp_daily_corr/temp_timeseries + plot_annotation(title = "Daily Temperature"))

png(here("output", "temp_plot_daily.png"),width = 6, height = 8, units = "in", res = 300)
temp_plots
dev.off()

## turbidity -------
# boxplots
ggplot(data_daily_complete %>% filter(turb_LIS < 100)) + geom_boxplot(aes(x = factor(year(date)), y= turb_LIS, fill = factor(month(date))))
ggplot(data_daily_complete %>% filter(turb_TOE < 100)) + geom_boxplot(aes(x = factor(year(date)), y= turb_TOE, fill = factor(month(date))))

(turb_daily_corr <- ggplot(data_daily_complete) + geom_point(aes(turb_LIS, turb_TOE)) + theme_bw())

(turb_timeseries <- ggplot(data_daily_complete) +
  geom_point(aes(date, turb_LIS), color = "steelblue4") +
  geom_point(aes(date, turb_TOE), color = "goldenrod") +
  theme_bw())

(turb_plots <- turb_daily_corr/turb_timeseries + plot_annotation(title = "Daily Turbidity"))

png(here("output", "turb_plots_daily.png"),width = 6, height = 8, units = "in", res = 300)
turb_plots
dev.off()

# Correlations ----------------------------------------------------

turb <- lm(turb_TOE ~ turb_LIS, data = data_daily_complete)
summary(turb)

temp <- lm(temp_TOE ~ temp_LIS, data = data_daily_complete)
summary(temp)


