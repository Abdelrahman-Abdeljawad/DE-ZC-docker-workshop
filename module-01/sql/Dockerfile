# base image we are building from
FROM python:3.13-slim

# set up working directory
WORKDIR /code

# add virtual environment to path
ENV PATH="/code/.venv/bin:$PATH"

# copy uv binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# copy uv files
COPY .python-version pyproject.toml uv.lock ./

# install dependencies from .lock file
RUN uv sync --locked

# copy ingestion script
COPY get_data.py .

# run ingestion script when the container runs
ENTRYPOINT ["python", "-u", "get_data.py"]