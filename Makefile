include configuration/.env
export

COMPOSE_FILE        = infrastructure/docker-compose.yaml
ENV_FILE            = configuration/.env
FLINK_JAR           = /opt/flink/jobs/pageview-stream-processor-1.0-SNAPSHOT.jar
FLINK_CLASS         = com.pipeline.streaming.processor.PageviewAggregatorJob
COMPOSE             = docker compose -f $(COMPOSE_FILE) --env-file $(ENV_FILE)

KAFKA_TOPIC         = pageviews-raw
KAFKA_PARTITIONS    = 3
KAFKA_BROKER        = kafka-broker
KAFKA_INTERNAL_ADDR = localhost:29092

FLINK_REST_URL      = http://localhost:8081/overview

KAFKA_TIMEOUT       = 120
FLINK_TIMEOUT       = 120
POLL_INTERVAL       = 5

.PHONY: help build up wait-for-kafka create-topic wait-for-schema-registry wait-for-flink submit-job logs down all

help: ## Show this help menu
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Compile, run all tests (unit + MiniCluster + IT), build Fat Jar
	@echo ""
	@echo "========================================="
	@echo "  Building Java modules (with full test suite)..."
	@echo "========================================="
	@echo ""
	./mvnw clean verify
	@echo ""
	@echo "  Build complete. All tests passed."
	@echo ""

up: ## Start all Docker containers (rebuilds images to avoid stale cache)
	@echo ""
	@echo "========================================="
	@echo "  Starting Docker containers..."
	@echo "========================================="
	@echo ""
	$(COMPOSE) up -d --build --force-recreate
	@echo ""
	@echo "  Containers started in detached mode."
	@echo ""

wait-for-kafka: ## Poll Kafka broker until it is ready to accept connections
	@echo ""
	@echo "========================================="
	@echo "  Waiting for Kafka broker..."
	@echo "========================================="
	@echo ""
	@elapsed=0; \
	until docker exec $(KAFKA_BROKER) bash -c " \
		echo 'security.protocol=SASL_PLAINTEXT' > /tmp/admin.properties && \
		echo 'sasl.mechanism=PLAIN' >> /tmp/admin.properties && \
		echo 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$(KAFKA_ADMIN_USERNAME)\" password=\"$(KAFKA_ADMIN_PASSWORD)\";' >> /tmp/admin.properties && \
		kafka-broker-api-versions \
			--bootstrap-server $(KAFKA_INTERNAL_ADDR) \
			--command-config /tmp/admin.properties" > /dev/null 2>&1; do \
		if [ $$elapsed -ge $(KAFKA_TIMEOUT) ]; then \
			echo ""; \
			echo "  ERROR: Kafka broker did not become ready within $(KAFKA_TIMEOUT)s."; \
			echo ""; \
			exit 1; \
		fi; \
		echo "  Still waiting... ($${elapsed}s elapsed)"; \
		sleep $(POLL_INTERVAL); \
		elapsed=$$((elapsed + $(POLL_INTERVAL))); \
	done
	@echo ""
	@echo "  Kafka broker is ready."
	@echo ""

create-topic: ## Create the pageviews-raw Kafka topic (3 partitions)
	@echo ""
	@echo "========================================="
	@echo "  Creating Kafka topic '$(KAFKA_TOPIC)'..."
	@echo "========================================="
	@echo ""
	@docker exec $(KAFKA_BROKER) bash -c " \
		echo 'security.protocol=SASL_PLAINTEXT' > /tmp/admin.properties && \
		echo 'sasl.mechanism=PLAIN' >> /tmp/admin.properties && \
		echo 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$(KAFKA_ADMIN_USERNAME)\" password=\"$(KAFKA_ADMIN_PASSWORD)\";' >> /tmp/admin.properties && \
		kafka-topics \
			--bootstrap-server $(KAFKA_INTERNAL_ADDR) \
			--command-config /tmp/admin.properties \
			--create \
			--if-not-exists \
			--topic $(KAFKA_TOPIC) \
			--partitions $(KAFKA_PARTITIONS) \
			--replication-factor 1"
	@echo ""
	@echo "  Topic '$(KAFKA_TOPIC)' is ready ($(KAFKA_PARTITIONS) partitions)."
	@echo ""

wait-for-flink: ## Poll the Flink REST API until it responds 200
	@echo ""
	@echo "========================================="
	@echo "  Waiting for Flink JobManager..."
	@echo "========================================="
	@echo ""
	@elapsed=0; \
	until curl -sf $(FLINK_REST_URL) > /dev/null 2>&1; do \
		if [ $$elapsed -ge $(FLINK_TIMEOUT) ]; then \
			echo ""; \
			echo "  ERROR: Flink JobManager did not become ready within $(FLINK_TIMEOUT)s."; \
			echo ""; \
			exit 1; \
		fi; \
		echo "  Still waiting... ($${elapsed}s elapsed)"; \
		sleep $(POLL_INTERVAL); \
		elapsed=$$((elapsed + $(POLL_INTERVAL))); \
	done
	@echo ""
	@echo "  Flink JobManager is ready."
	@echo ""

wait-for-schema-registry: ## Poll the Schema Registry until it responds
	@echo ""
	@echo "========================================="
	@echo "  Waiting for Schema Registry..."
	@echo "========================================="
	@echo ""
	@elapsed=0; \
	until curl -sf http://localhost:8085/subjects > /dev/null 2>&1; do \
		if [ $$elapsed -ge $(FLINK_TIMEOUT) ]; then \
			echo ""; \
			echo "  ERROR: Schema Registry did not become ready within $(FLINK_TIMEOUT)s."; \
			echo ""; \
			exit 1; \
		fi; \
		echo "  Still waiting... ($${elapsed}s elapsed)"; \
		sleep $(POLL_INTERVAL); \
		elapsed=$$((elapsed + $(POLL_INTERVAL))); \
	done
	@echo ""
	@echo "  Schema Registry is ready."
	@echo ""

submit-job: ## Submit the Flink aggregation job to the local cluster
	@echo ""
	@echo "========================================="
	@echo "  Submitting Flink job..."
	@echo "========================================="
	@echo ""
	docker exec flink-jobmanager flink run -d \
		-c $(FLINK_CLASS) $(FLINK_JAR) \
		--bootstrap.servers kafka-broker:29092 \
		--kafka.topic $(KAFKA_TOPIC) \
		--kafka.group.id flink-aggregator-group \
		--schema.registry.url http://schema-registry:8081 \
		--raw.output.path /opt/flink/output/raw \
		--agg.output.path /opt/flink/output/aggregated \
		--dlq.output.path /opt/flink/output/dlq
	@echo ""
	@echo "  Flink job submitted!"
	@echo ""
	@echo "  Dashboard:         http://localhost:8081"
	@echo "  Kafka UI:          http://localhost:7070"
	@echo "  Schema Registry:   http://localhost:8085/subjects/"
	@echo "  Prometheus:        http://localhost:9090"
	@echo "  Grafana:           http://localhost:3000"
	@echo ""

logs: ## Tail the logs for the Flink TaskManager
	$(COMPOSE) logs -f taskmanager

down: ## Stop and remove all Docker containers
	@echo ""
	@echo "========================================="
	@echo "  Stopping all containers..."
	@echo "========================================="
	@echo ""
	$(COMPOSE) down
	@echo ""
	@echo "  All containers stopped."
	@echo ""

all: build up wait-for-kafka create-topic wait-for-schema-registry wait-for-flink submit-job
	@echo ""
	@echo "========================================="
	@echo "  Pipeline is running!"
	@echo "========================================="
	@echo ""
	@echo "  Raw events  -> output/raw/"
	@echo "  Aggregated  -> output/aggregated/"
	@echo "  DLQ         -> output/dlq/"
	@echo ""
	@echo "  Run 'make logs' to tail the Flink TaskManager logs."
	@echo ""
