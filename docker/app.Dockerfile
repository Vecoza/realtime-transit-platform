# One image for every platform process: producer, sink, aggregator, the one-off init and the
# scheduled batch jobs (GTFS refresh, storage lifecycle and dbt, run by supercronic).
FROM python:3.11-slim-bookworm

ARG SUPERCRONIC_VERSION=v0.2.49
ARG TARGETARCH
RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && case "${TARGETARCH:-amd64}" in \
         amd64) sum=e63c11a9726b775a6a11801e81af4f3fb926aa68 ;; \
         arm64) sum=0b6c5bb743e0b0dafed1132198c81807927ac413 ;; \
         *) echo "unsupported architecture ${TARGETARCH}" && exit 1 ;; \
       esac \
    && curl -fsSLo /usr/local/bin/supercronic \
       "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-${TARGETARCH:-amd64}" \
    && echo "${sum}  /usr/local/bin/supercronic" | sha1sum -c - \
    && chmod +x /usr/local/bin/supercronic

RUN useradd --create-home --uid 1000 app
WORKDIR /app
# A volume mounted here starts out owned by the app user.
RUN mkdir data && chown app:app data

# Dependencies first, in their own layer: a code change then rebuilds without the network.
COPY pyproject.toml constraints.txt README.md ./
RUN python -c "import tomllib; p = tomllib.load(open('pyproject.toml', 'rb'))['project']; \
print('\n'.join(p['dependencies'] + p['optional-dependencies']['analytics']))" > requirements.txt \
    && pip install --no-cache-dir -c constraints.txt -r requirements.txt \
    && rm requirements.txt
# DuckDB's PostgreSQL extension, installed for the app user so that the jobs download nothing.
USER app
RUN python -c "import duckdb; duckdb.connect().install_extension('postgres')"
USER root

COPY src src
COPY gtfs gtfs
COPY producer producer
COPY processor processor
COPY archive archive
COPY analytics analytics
RUN pip install --no-cache-dir --no-deps --no-build-isolation .

COPY alembic.ini ./
COPY db db
COPY --chown=app dbt dbt
COPY docker/crontab crontab
USER app

ENV TZ=UTC
CMD ["supercronic", "-passthrough-logs", "/app/crontab"]
