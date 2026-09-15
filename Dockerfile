FROM python:3.11-slim
# Install system dependencies for python's rasterio and gdal packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    libexpat1 \
    libgdal-dev \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*
RUN pip install poetry

WORKDIR .
COPY pyproject.toml poetry.lock /
RUN poetry config virtualenvs.create false
RUN poetry install --no-root --no-interaction
COPY floodpipeline /floodpipeline
COPY data_updates /data_updates
COPY tests /tests
COPY config /config
COPY "flood_pipeline.py" .
COPY "run_scenario.py" .

# ENTRYPOINT ["poetry", "run", "python", "-m", "flood_pipeline"]