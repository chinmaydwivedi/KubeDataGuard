COMPOSE ?= docker compose
PYTHON ?= python3
KIND ?= kind
KUBECTL ?= kubectl
KIND_CLUSTER ?= kubedataguard
DEMO_IMAGE ?= kubedataguard-dataguard:latest
OPERATOR_IMAGE ?= kubedataguard-operator:latest
APP = $(COMPOSE) run --rm dataguard
K8S_DEMO_ENV = --env=POSTGRES_DSN=postgresql://dataguard:dataguard@dataguard-postgres:5432/dataguard --env=KAFKA_BOOTSTRAP_SERVERS=dataguard-redpanda:9092 --env=ORDER_EVENTS_TOPIC=orders.events --env=OPENSEARCH_URL=http://dataguard-opensearch:9200 --env=ORDERS_INDEX=orders --env=REDIS_URL=redis://dataguard-redis:6379/0 --env=ORDER_CACHE_PREFIX=order --env=CLICKHOUSE_URL=http://dataguard-clickhouse:8123 --env=CLICKHOUSE_USER=dataguard --env=CLICKHOUSE_PASSWORD=dataguard --env=CLICKHOUSE_DATABASE=dataguard --env=CLICKHOUSE_TABLE=orders_analytics --env=REPORT_DIR=/tmp/dataguard-reports

.PHONY: up up-object-store up-analytics down reset init seed generate index cache-index analytics-init analytics-backfill drift drift-cache drift-freshness drift-clickhouse check check-aggregate check-checksum check-freshness check-redis-freshness check-clickhouse-aggregate check-s3 repair repair-freshness demo-local demo-drift demo-freshness-drift demo-clickhouse-drift demo-repair test operator-test operator-build operator-image kind-create k8s-demo-images k8s-demo-stack k8s-demo-resources k8s-demo-seed k8s-demo-drift k8s-demo-force-drift-check k8s-demo-reset-slo k8s-demo-wait-checks k8s-demo-status k8s-demo

up:
	$(COMPOSE) up -d postgres redpanda-0 redpanda-console opensearch redis

up-object-store:
	$(COMPOSE) --profile object-store up -d minio minio-mc

up-analytics:
	$(COMPOSE) --profile analytics up -d clickhouse

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v

init:
	$(APP) init

seed:
	$(APP) init --reset
	$(APP) generate --count 50 --paid-ratio 0.8

generate:
	$(APP) generate --count 50 --paid-ratio 0.7

index:
	$(APP) index --max-messages 50

cache-index:
	$(APP) cache-index --max-messages 50

analytics-init:
	$(APP) analytics-init --reset

analytics-backfill:
	$(APP) analytics-backfill

drift:
	$(APP) index --max-messages 50 --skip-every-paid 5

drift-cache:
	$(APP) cache-index --max-messages 50 --skip-every-paid 5

drift-freshness:
	$(APP) inject-freshness-drift --count 5 --source-age-seconds 120 --target-staleness-seconds 30

drift-clickhouse:
	$(APP) analytics-init --reset
	$(APP) analytics-backfill --skip-every-paid 5

check:
	$(APP) check --max-lag-seconds 0 --write-report

check-aggregate:
	$(APP) check --invariant aggregate --max-lag-seconds 0 --write-report

check-checksum:
	$(APP) check --invariant checksum --checksum-prefix-length 1 --max-lag-seconds 0 --write-report

check-freshness:
	$(APP) check --invariant freshness --max-lag-seconds 60 --write-report

check-redis-freshness:
	$(APP) check --invariant redis-freshness --max-lag-seconds 60 --write-report

check-clickhouse-aggregate:
	$(APP) check --invariant clickhouse-aggregate --max-lag-seconds 0 --write-report

check-s3:
	REPORT_STORE=s3 REPORT_BUCKET=kubedataguard-reports REPORT_PREFIX=local REPORT_S3_ENDPOINT_URL=http://minio:9000 AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin $(APP) check --max-lag-seconds 0 --write-report

repair:
	$(APP) repair --verify

repair-freshness:
	$(APP) repair --verify --verify-invariant freshness --max-lag-seconds 60

demo-local:
	PYTHONPATH=src $(PYTHON) -m dataguard.cli demo-local

demo-drift:
	$(APP) init --reset
	$(APP) generate --count 50 --paid-ratio 0.8
	$(APP) index --max-messages 50 --skip-every-paid 5
	$(APP) check --max-lag-seconds 0 --write-report

demo-freshness-drift:
	$(APP) init --reset
	$(APP) generate --count 50 --paid-ratio 0.8
	$(APP) index --max-messages 50
	$(APP) inject-freshness-drift --count 5 --source-age-seconds 120 --target-staleness-seconds 30
	$(APP) check --invariant freshness --max-lag-seconds 60 --write-report

demo-clickhouse-drift:
	$(COMPOSE) --profile analytics up -d clickhouse
	$(APP) init --reset
	$(APP) generate --count 50 --paid-ratio 0.8
	$(APP) analytics-init --reset
	$(APP) analytics-backfill --skip-every-paid 5
	$(APP) check --invariant clickhouse-aggregate --max-lag-seconds 0 --write-report

demo-repair:
	$(APP) repair --verify

test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests

operator-test:
	go test ./...

operator-build:
	go build ./cmd/dataguard-operator

operator-image:
	docker build -f Dockerfile.operator -t $(OPERATOR_IMAGE) .

kind-create:
	$(KIND) get clusters | grep -qx $(KIND_CLUSTER) || $(KIND) create cluster --name $(KIND_CLUSTER) --wait 180s

k8s-demo-images:
	docker build -t $(DEMO_IMAGE) .
	docker build -f Dockerfile.operator -t $(OPERATOR_IMAGE) .
	$(KIND) load docker-image $(DEMO_IMAGE) --name $(KIND_CLUSTER)
	$(KIND) load docker-image $(OPERATOR_IMAGE) --name $(KIND_CLUSTER)

k8s-demo-stack:
	$(KUBECTL) delete deployment dataguard-postgres dataguard-redpanda dataguard-opensearch dataguard-redis dataguard-clickhouse --ignore-not-found
	$(KUBECTL) apply -f k8s/demo-stack.yaml
	$(KUBECTL) rollout status statefulset/dataguard-postgres --timeout=180s
	$(KUBECTL) rollout status statefulset/dataguard-redpanda --timeout=180s
	$(KUBECTL) rollout status statefulset/dataguard-opensearch --timeout=300s
	$(KUBECTL) rollout status statefulset/dataguard-redis --timeout=180s
	$(KUBECTL) rollout status statefulset/dataguard-clickhouse --timeout=300s

k8s-demo-resources:
	$(KUBECTL) apply -f k8s/crds
	$(KUBECTL) apply -f examples/commerce-consistency.yaml
	$(KUBECTL) apply -f k8s/operator.yaml
	$(KUBECTL) rollout status deployment/kubedataguard-operator --timeout=180s

k8s-demo-seed:
	$(KUBECTL) delete pod dataguard-demo-seed --ignore-not-found
	$(KUBECTL) run dataguard-demo-seed --image=$(DEMO_IMAGE) --image-pull-policy=IfNotPresent --restart=Never $(K8S_DEMO_ENV) --command -- /bin/sh -c 'python -m dataguard.cli init --reset && python -m dataguard.cli generate --count 50 --paid-ratio 0.8 && python -m dataguard.cli analytics-init --reset && python -m dataguard.cli analytics-backfill'
	$(KUBECTL) wait --for=jsonpath='{.status.phase}'=Succeeded pod/dataguard-demo-seed --timeout=300s
	$(KUBECTL) logs pod/dataguard-demo-seed

k8s-demo-drift:
	$(KUBECTL) delete pod dataguard-demo-drift --ignore-not-found
	$(KUBECTL) run dataguard-demo-drift --image=$(DEMO_IMAGE) --image-pull-policy=IfNotPresent --restart=Never $(K8S_DEMO_ENV) --command -- /bin/sh -c 'python -m dataguard.cli index --max-messages 50 --skip-every-paid 5'
	$(KUBECTL) wait --for=jsonpath='{.status.phase}'=Succeeded pod/dataguard-demo-drift --timeout=300s
	$(KUBECTL) logs pod/dataguard-demo-drift

k8s-demo-force-drift-check:
	$(KUBECTL) patch invariant paid-orders-indexed --type=merge -p '{"spec":{"maxLagSeconds":0}}'
	$(KUBECTL) patch invariant paid-orders-aggregate --type=merge -p '{"spec":{"maxLagSeconds":0}}'
	$(KUBECTL) patch invariant paid-orders-checksum --type=merge -p '{"spec":{"maxLagSeconds":0,"checksumPrefixLength":1}}'
	$(KUBECTL) patch invariant paid-orders-redis-cache-freshness --type=merge -p '{"spec":{"maxLagSeconds":0}}'
	$(KUBECTL) wait --for=jsonpath='{.status.phase}'=DriftDetected invariant/paid-orders-indexed --timeout=300s
	$(KUBECTL) wait --for=jsonpath='{.status.phase}'=DriftDetected invariant/paid-orders-aggregate --timeout=300s
	$(KUBECTL) wait --for=jsonpath='{.status.phase}'=DriftDetected invariant/paid-orders-checksum --timeout=300s
	$(KUBECTL) wait --for=jsonpath='{.status.phase}'=DriftDetected invariant/paid-orders-redis-cache-freshness --timeout=300s

k8s-demo-reset-slo:
	$(KUBECTL) patch invariant paid-orders-indexed --type=merge -p '{"spec":{"maxLagSeconds":60}}'
	$(KUBECTL) patch invariant paid-orders-aggregate --type=merge -p '{"spec":{"maxLagSeconds":60}}'
	$(KUBECTL) patch invariant paid-orders-checksum --type=merge -p '{"spec":{"maxLagSeconds":60}}'

k8s-demo-wait-checks:
	$(KUBECTL) wait --for=condition=complete job -l app.kubernetes.io/name=kubedataguard --timeout=300s

k8s-demo-status:
	$(KUBECTL) get invariant -o custom-columns=NAME:.metadata.name,PHASE:.status.phase,DRIFT:.status.driftCount,CHECK:.status.checkID,REPORT:.status.reportRef
	$(KUBECTL) get jobs -l app.kubernetes.io/name=kubedataguard

k8s-demo: kind-create k8s-demo-images k8s-demo-stack k8s-demo-seed k8s-demo-drift k8s-demo-resources k8s-demo-force-drift-check k8s-demo-wait-checks k8s-demo-status
