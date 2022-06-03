FROM postgres:13

COPY . /opt/runops
COPY db/* /docker-entrypoint-initdb.d

RUN apt update && \
    apt install -y --no-install-recommends  \
      postgresql-13-python3-multicorn  \
      postgresql-plpython3-13  \
      python3-pip && \
    cd /opt/runops && \
    pip install pypika && \
    pip install -e .
