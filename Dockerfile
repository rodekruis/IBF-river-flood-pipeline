FROM python:3.11-slim
RUN pip install poetry

# add credentials and install SML pipeline
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