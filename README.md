# IBF-river-flood-pipeline

Forecast riverine flooding. Part of [IBF-system](https://github.com/rodekruis/IBF-system).

## Description

The pipeline roughly consists of three steps:
* Extract data on [river discharge](https://en.wikipedia.org/wiki/Discharge_(hydrology)) from an external provider, both in specific locations (_stations_) and over pre-defined areas (_administrative divisions_).
* Forecast floods by determining if the river discharge is higher than pre-defined _thresholds_; if so, calculate flood extent and impact (_affected people_).
* Send this data to the IBF app.

The pipeline stores data in:
* [ibf-cosmos](https://portal.azure.com/#@rodekruis.onmicrosoft.com/resource/subscriptions/57b0d17a-5429-4dbb-8366-35c928e3ed94/resourceGroups/IBF-system/overview) (Azure Cosmos DB): river discharge per station / administrative division, flood forecasts, and trigger thresholds
* [510ibfsystem](https://portal.azure.com/#@rodekruis.onmicrosoft.com/resource/subscriptions/57b0d17a-5429-4dbb-8366-35c928e3ed94/resourceGroups/IBF-system/providers/Microsoft.Storage/storageAccounts/510ibfsystem/overview) (Azure Storage Account): raw data from GloFAS and other geospatial data 

The pipeline depends on the following services:
* [GloFAS](https://global-flood.emergency.copernicus.eu/): provides river discharge forecasts
* [Glofas data pipeline in IBF-data-factory](https://adf.azure.com/en/authoring/pipeline/GloFAS%20data%20pipeline?factory=%2Fsubscriptions%2F57b0d17a-5429-4dbb-8366-35c928e3ed94%2FresourceGroups%2FIBF-system%2Fproviders%2FMicrosoft.DataFactory%2Ffactories%2FIBF-data-factory) (Azure Data Factory): extracts GloFAS data and stores it in `510ibfsystem`
* IBF-app 

For more information, see the [functional architecture diagram](https://miro.com/app/board/uXjVK7Valso=/?moveToWidget=3458764592859255828&cot=14).

## Basic Usage

To run the pipeline locally
1. fill in the secrets in `.env.example` and rename the file to `.env`; in this way, they will be loaded as environment variables
2. install requirements
```
pip install poetry
poetry install --no-interaction
```
3. run the pipeline with `python flood_pipeline.py`
```
Usage: flood_pipeline.py [OPTIONS]

Options:
  --country TEXT  country ISO3
  --prepare       prepare discharge data
  --extract       extract discharge data
  --forecast      forecast floods
  --send          send to IBF app
  --save          save to storage
  --help          Show this message and exit.
```

## Advanced Usage

### How do I set up the pipeline for a new country?

1. Check that the administrative boundaries are in the IBF system; if not, ask IBF developers to add them
2. Add country-specific configuration in `config/config.yaml`
3. Create historical flood extent maps
```
python data_updates\add_flood_maps.py --country <country ISO3>
```
3. Compute trigger and alert thresholds
```
python data_updates\add_flood_thresholds.py --country <country ISO3>
```
4. Update [`Glofas data pipeline`](https://adf.azure.com/en/authoring/pipeline/GloFAS%20data%20pipeline?factory=%2Fsubscriptions%2F57b0d17a-5429-4dbb-8366-35c928e3ed94%2FresourceGroups%2FIBF-system%2Fproviders%2FMicrosoft.DataFactory%2Ffactories%2FIBF-data-factory) in IBF-data-factory so that it will trigger a pipeline run for the new country

### How do I insert an exception for a specific country?

You don't. The pipeline is designed to work in the same way for all countries.
If you need to change the pipeline's behavior for a specific country, please discuss your needs with your fellow data specialist, they will try their best to accommodate your request.

### There is a new version of GloFAS, how do I update the pipeline?

GloFAS should take care to update the river discharge data in a backward-compatible way. If that is not the case, you need 
to have a look at `floodpipeline/extract.py` and change what's needed.

What will probably change with the new GloFAS version are the trigger/alert thresholds. To update them you need to
1. Change the 


