### Hey there, this is the repository for a forecasting service based in SARIMAX model. The main goal is to forecast the expected revenue and product demand for a given kiosk or product, sampled on a daily basis.

### This way the user can get an idea of expected demand and revenue, such that proper inventory management and resource allocation can be done.

### Services Provided - 
#### ! Input received from the client - OrganizationId, KioskId, ProductIds, Forecast Horizon

#### 1. Stat cards (predicted net sales, predicted net demand, expected number of customers, forecast accuracy)
#### 2. Aggregated revenue f=orecast
#### 3. Forecasted Revenue comparision of selected kiosks
#### 4. Revenue percentage change (history vs future)
#### 5. Product Demand Forecasts
#### 6. Product Demand percentage change
#### 7. Top selling kiosks by forecasted revenue
#### 8. Top selling products by forecasted demand

# How to run:

1. `pip install -r requirements.txt`
2. `python triggerDataPipeline.py` -> This is the core of the project, it will initiate data migration from mongoDB, data imputation, will include holiday information (national,regional), validate training data sufficiency, initiate the modelling, create forecasts, find accuracy, store them back in BigQuery. (Total runtime ~ 2 hours)
3. `python app.py` -> This will run the flask application and will host the forecasting application on localhost, send the payload via Postman in Json body and get the results in the 8 endpoints mentioned above.

### Future Work - Introduction of deep learning and transformer based ensemble modelling, asynchronous calling, batching, model retraining.

##### Have Fun Forecasting...