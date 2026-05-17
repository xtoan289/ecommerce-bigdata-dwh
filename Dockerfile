FROM apache/airflow:2.7.1

USER root
# Install OpenJDK-11
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         default-jre-headless \
         procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
# Copy requirements and install
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
