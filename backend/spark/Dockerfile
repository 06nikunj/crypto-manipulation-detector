FROM apache/spark:4.2.0-scala2.13-java21-python3-ubuntu

USER root
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/opt/cryptosentinel
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3
ENV SPARK_NO_DAEMONIZE=true

WORKDIR /opt/cryptosentinel
COPY backend/spark/requirements.txt /tmp/requirements.txt

RUN python3 -m pip install --no-cache-dir --upgrade pip && python3 -m pip install --no-cache-dir -r /tmp/requirements.txt && rm -f /tmp/requirements.txt

COPY --chown=spark:spark backend /opt/cryptosentinel/backend
COPY --chown=spark:spark models /opt/cryptosentinel/models
COPY --chown=spark:spark data /opt/cryptosentinel/data

RUN mkdir -p /var/lib/cryptosentinel/checkpoints /tmp/.ivy2 && chown -R spark:spark /var/lib/cryptosentinel /tmp/.ivy2

USER spark
EXPOSE 8090
ENTRYPOINT ["/opt/spark/bin/spark-submit"]
