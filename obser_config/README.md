This docker compose deployment includes some observability tools like Loki, Tempo and Prometheus.

```bash
cd obser_config
```

```bash
docker compose up -d --build
```

You know can access localhost:3000 and see all the logs, traces and metrics.