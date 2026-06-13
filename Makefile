COMPOSE ?= docker compose
PYTHON ?= python3
APP = $(COMPOSE) run --rm dataguard

.PHONY: up up-object-store down reset init seed generate index drift drift-freshness check check-aggregate check-freshness check-s3 repair repair-freshness demo-local demo-drift demo-freshness-drift demo-repair test operator-test operator-build operator-image

up:
	$(COMPOSE) up -d postgres redpanda-0 redpanda-console opensearch

up-object-store:
	$(COMPOSE) --profile object-store up -d minio minio-mc

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

drift:
	$(APP) index --max-messages 50 --skip-every-paid 5

drift-freshness:
	$(APP) inject-freshness-drift --count 5 --source-age-seconds 120 --target-staleness-seconds 30

check:
	$(APP) check --max-lag-seconds 0 --write-report

check-aggregate:
	$(APP) check --invariant aggregate --max-lag-seconds 0 --write-report

check-freshness:
	$(APP) check --invariant freshness --max-lag-seconds 60 --write-report

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

demo-repair:
	$(APP) repair --verify

test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s tests

operator-test:
	go test ./...

operator-build:
	go build ./cmd/dataguard-operator

operator-image:
	docker build -f Dockerfile.operator -t kubedataguard-operator:latest .
