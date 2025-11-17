Here’s a sample `README.md` you can use for your project:

***

# **NYC TLC Taxi Demand Forecasting**

## **Overview**

This project analyzes **NYC Taxi and Limousine Commission (TLC) Trip Record Data** for **Yellow and Green taxis** to forecast daily ride demand using time series models. The project will be expanded to include geographical and machine learning models. The dataset includes trip-level details such as pickup/drop-off timestamps, locations, distances, fares, payment types, and passenger counts. Data is provided by technology vendors under TLC’s TPEP/LPEP programs; TLC does not guarantee accuracy.

We aggregate hourly trip data into **daily totals** and apply statistical and machine learning models (ARIMA, SARIMA, and Prophet) to predict future demand.

***

## **Data Source**

*   **NYC TLC Trip Records**:  
    <https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf>  
    <https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf>

Fields include:

*   Pickup and drop-off **date/time**
*   Pickup and drop-off **locations**
*   **Trip distance**
*   **Fare details** (base fare, taxes, surcharges)
*   **Payment type**
*   **Passenger count**

***

## **Project Workflow**

### 1. **Data Preparation**

*   Load hourly demand data from TLC trip records.
*   Aggregate hourly rides into **daily totals**.
*   Handle missing dates using **linear interpolation**.

### 2. **Exploratory Analysis**

*   Plot **ACF and PACF** to identify autocorrelation and seasonality.
*   Detect weekly seasonality (7-day cycle).

### 3. **Modeling**

*   **ARIMA**: Non-seasonal model for trend and short-term autocorrelation.
*   **SARIMA**: Seasonal ARIMA with weekly seasonality.
*   **Prophet**: Handles trend, seasonality, and holidays.

### 4. **Evaluation**

*   Metrics:
    *   **MAE**: Mean Absolute Error
    *   **RMSE**: Root Mean Squared Error
    *   **MAPE**: Mean Absolute Percentage Error  
        Example:

    <!---->

        MAE:  11125.95
        RMSE: 13720.65
        MAPE: 7.78%
    Interpretation: MAPE under 10% indicates good forecast accuracy, though RMSE suggests large deviations on peak days.

***

## **Key Insights**

*   ACF/PACF analysis suggests **ARIMA(1,1,1)** or **ARIMA(1,1,2)**.
*   Weekly seasonality → **SARIMA(1,1,1)(1,0,1,7)** performs better.
*   Prophet is useful for capturing trend and seasonality with minimal tuning.

***

## **How to Run**

1.  Install dependencies:
    ```bash
    pip install pandas matplotlib statsmodels pmdarima prophet pyarrow
    ```
2.  Place TLC trip data in `data/hourly_demand_partitioned/final`.
3.  Run the main script:
    ```bash
    python forecast.py
    ```
4.  Outputs:
    *   Forecast plots
    *   Metrics summary
    *   Saved results in `data/forecast_results`

***
