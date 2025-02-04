library(forecast)
library(jsonlite)
library(tidyr)
library(dplyr)

# copy the parameters from the subprocess file
args <- commandArgs(trailingOnly = TRUE)

# put the results in a dataframe
preprocess_data <- read.delim(text = args[1], header = TRUE, col.names = c("revenue"))
# cleaning of the data in the data frame
result <- preprocess_data %>%
  filter(revenue != "Date") %>% # Updated condition
  separate(revenue, into = c("index","Date","revenue"), sep = "\\s{2,}") %>% # nolint
  mutate(index = as.numeric(index), Date = as.Date(Date, format = "%Y-%m-%d"), revenue = as.numeric(revenue)) %>% # nolint
  filter(!is.na(Date))

keeps <- c("Date","revenue")
df <- result[keeps]

# copy the parameters from the subprocess file
forecast_points <- as.integer(args[2])

# function to Format the timeseries data
prepare_timeseries <- function(data, freq) {
  start_date <- as.numeric(format(min(data$Date), "%Y"))
  start_day <- as.numeric(format(min(data$Date), "%j"))
  new_df_ts <- ts(data$revenue, start = c(start_date, start_day), frequency = freq) # nolint
  return(new_df_ts)
}

# function to  train the model
train_arima <- function(ts_data) { # nolint

  fit <- auto.arima(ts_data) # nolint
  return(fit)
}

# function to forecast and format the forecasted results
generate_forecasts <- function(model, data) {
  forecasts <- forecast(model, h = forecast_points, level = c(95))
  last_date_in_data <- max(data$Date) # Find the maximum date in the 'Date' column
  forecast_dates <- seq(last_date_in_data + 1, by = "days", length.out = length(forecasts$mean)) # nolint: line_length_linter.
  forecast_data <- data.frame(Date = forecast_dates, Forecast = forecasts$mean, Upper_bound = forecasts$upper, Lower_bound = forecasts$lower) # nolint
  forecast_data[forecast_data < 0] <- 0
  return(forecast_data)
}

forecast_gross_price <- function(data, freq = 7) { # nolint
  ts_data <- prepare_timeseries(data, freq) # nolint
  model <- train_arima(ts_data) # nolint
  forecast_data <- generate_forecasts(model, data)
  return(forecast_data)
}

result <- forecast_gross_price(df)
print(result)
