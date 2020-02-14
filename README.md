# Build source
```bash
make package
```

# Build docker
```bash
make build-docker-from-local
```

# kafka, zookeeper, schema registry, connect start 
```bash
docker-compose up -d --build
```

# Connector run
```bash
curl -X POST -H "Content-Type: application/json" --data @config/connector_l2ptn_ftp_pm.config http://localhost:8083/connectors
```

# Check Run Connector

*  Control Center
```bash
http://localhost:9021
```

*  Consumer
```bash
docker-compose exec connect kafka-console-consumer --topic l2ptntunnel --bootstrap-server kafka:29092  --property print.key=true --from-beginning
```