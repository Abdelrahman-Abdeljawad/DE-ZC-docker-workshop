# base docker image that we are building from
FROM python:3.13.11-slim

# set up working directory in the image
WORKDIR /code

# add virtual environment to path
ENV PATH="/code/.venv/bin:$PATH"

# get uv (copy uv binary) from the official uv image
# (multistage-build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/
COPY .python-version pyproject.toml uv.lock ./

# so dependencies in the container are the same on our machine
# install dependencies from .lock file
RUN uv sync --locked

# copy from host machine to the image
COPY pipeline.py .

# define what to do when the container runs
ENTRYPOINT ["python", "pipeline.py"]