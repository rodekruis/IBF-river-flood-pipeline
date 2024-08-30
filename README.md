# IBF-river-flood-pipeline

Forecast river or fluvial flooding. Part of [IBF-system](https://github.com/rodekruis/IBF-system).

## Description

The pipeline roughly consists of three steps:
* Extract data on river [discharge](https://en.wikipedia.org/wiki/Discharge_(hydrology)) from an external provider (GloFAS), both in specific locations (`stations`) and over administrative areas.
* Forecast river floods by determining if the river discharge is higher than pre-defined `thresholds`; if so, calculate flood extent and impact (affected people).
* Send this data to the IBF app.

See functional architecture in the image below.

## Basic Usage

To run the pipeline locally
1. fill in the secrets in `example.env` and rename the file to `.env`; in this way, they will be loaded as environment variables
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

### How do I set up a new country?

First, create the historical flood extent maps that the pipeline needs to create flood extents
```
python data_updates\add_flood_maps.py --country KEN
```
## 

![image](https://github.com/user-attachments/assets/798f0641-704e-48b5-96d7-963a78d83b58)
