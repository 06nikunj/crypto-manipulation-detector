FROM grafana/grafana:12.4.2

COPY provisioning /etc/grafana/provisioning
COPY dashboards /var/lib/grafana/dashboards

LABEL org.opencontainers.image.title="CryptoSentinel Grafana" \
      org.opencontainers.image.description="Provisioned Grafana observability for actual InfluxDB pipeline metrics"

EXPOSE 3000
