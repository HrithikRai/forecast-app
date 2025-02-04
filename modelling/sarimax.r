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
  filter(revenue != "Date") %>%
  separate(revenue, into = c("index", "Date", "revenue", "national_holiday", "regional_holiday"), sep = "\\s{2,}") %>%
  mutate(index = as.numeric(index), Date = as.Date(Date, format = "%Y-%m-%d"), revenue = as.numeric(revenue), national_holiday = as.numeric(national_holiday), regional_holiday = as.numeric(regional_holiday)) %>%
  filter(!is.na(Date))

keeps <- c("Date", "revenue", "national_holiday", "regional_holiday")
df <- result[keeps]

# copy the parameters from the subprocess file
forecast_points <- as.integer(args[2])

# function to Format the timeseries data
prepare_timeseries <- function(data, freq) {
  start_date <- as.numeric(format(min(data$Date), "%Y"))
  start_day <- as.numeric(format(min(data$Date), "%j"))
  new_df_ts <- ts(data$revenue, start = c(start_date, start_day), frequency = freq)
  return(new_df_ts)
}

# function to train the model
train_arima <- function(ts_data, exog_variables) {
  fit <- auto.arima(ts_data, xreg = exog_variables)
  return(fit)
}

# function to forecast and format the forecasted results
generate_forecasts <- function(model, data, forecast_points, exog_variables) {
  forecasts <- forecast(model, xreg = matrix(rep(colMeans(exog_variables, na.rm = TRUE), forecast_points), nrow = forecast_points, byrow = TRUE), h = forecast_points, level = c(95))
  last_date_in_data <- max(data$Date)
  forecast_dates <- seq(last_date_in_data + 1, by = "days", length.out = length(forecasts$mean))

  forecast_data <- data.frame(Date = forecast_dates, Forecast = forecasts$mean, Upper_bound = upper_bound, Lower_bound = lower_bound)
  forecast_data[forecast_data < 0] <- 0
  return(forecast_data)
}

# Forecasting steps
forecast_gross_price <- function(data, freq = 7) {
  ts_data <- prepare_timeseries(data, freq)
  exog_variables <- cbind(data$national_holiday, data$regional_holiday)
  model <- train_arima(ts_data, exog_variables)
  forecast_data <- generate_forecasts(model, data, forecast_points, exog_variables)
  return(forecast_data)
}

# return the final forecasted results back to the subprocess
result <- tryCatch(
  {
    forecast_gross_price(df)
  },
  error = function(e) {
    cat("Error during forecasting:", conditionMessage(e), "\n")
    NULL
  }
)

if (!is.null(result)) {
  #print(result)
} else {
  cat("No forecast data generated.\n")
}
