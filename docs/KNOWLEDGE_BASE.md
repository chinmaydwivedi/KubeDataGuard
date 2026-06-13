# Knowledge Base

This file records the external knowledge sources used to shape the MVP.

## Official Sources Consulted

- Kubernetes custom resources:
  - https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/
- Kubernetes controllers:
  - https://kubernetes.io/docs/concepts/architecture/controller/
- Kubernetes operator pattern:
  - https://kubernetes.io/docs/concepts/extend-kubernetes/operator/
- Docker Compose:
  - https://docs.docker.com/compose/
  - https://docs.docker.com/reference/compose-file/
- Redpanda single-broker Docker Compose lab:
  - https://docs.redpanda.com/labs/docker-compose/single-broker/
- OpenSearch Docker documentation:
  - https://docs.opensearch.org/latest/install-and-configure/install-opensearch/docker/

## Academic Sources Consulted

- DBLog: A Watermark Based Change-Data-Capture Framework:
  - https://arxiv.org/abs/2010.12597
  - Preserves the idea that a serious CDC verifier needs watermarks, chunk boundaries, and resumable snapshot-plus-log evidence.
- Polyglot Persistence in Microservices:
  - https://arxiv.org/abs/2509.08014
  - Frames the core problem as heterogeneous stores carrying one business truth across different persistence models.
- Towards Polyglot Data Stores:
  - https://arxiv.org/abs/2204.05779
  - Supports the need for explicit coordination and verification across heterogeneous data stores.
- Consistency models in distributed systems:
  - https://arxiv.org/abs/1902.03305
  - Grounds the invariant taxonomy in data-centric and client-centric consistency guarantees.
- A Unified Model of Non-transactional Consistency Levels:
  - https://arxiv.org/abs/2409.01576
  - Reinforces modeling guarantees as ordering constraints over histories.
- Elle: Inferring Isolation Anomalies from Experimental Observations:
  - https://arxiv.org/abs/2003.10554
  - Requires failed checks to include concise counterexamples, not only aggregate failure counts.
- Flo: a Semantic Foundation for Progressive Stream Processing:
  - https://arxiv.org/abs/2411.08274
  - Strengthens the freshness SLO model for derived streaming outputs.
- DCaaS: Data Consistency as a Service:
  - https://arxiv.org/abs/1306.0441
  - Acts as the early conceptual ancestor for externalized data-consistency policy.

## Design Facts Preserved

- Kubernetes custom resources extend the Kubernetes API and are the natural interface for declaring data consistency intent.
- Kubernetes controllers are reconciliation loops; KubeDataGuard reuses that mental model for data correctness.
- Docker Compose is the right first local packaging format because the MVP needs several cooperating services.
- Redpanda exposes a Kafka-compatible API and has an official single-broker Compose example suitable for local development.
- OpenSearch can run in a local single-node Docker setup, which is enough for the first derived-search-view demo.

## DDIA Ideas Preserved

- Derived data can lag or drift from the source of truth.
- Event streams and indexes are useful because they decouple write and read paths, but that decoupling creates repair obligations.
- A system may be operationally healthy and semantically wrong.
- Batch repair and stream processing often coexist in real systems.
- CDC checks should preserve what history range was observed: snapshot bounds, log offsets, and watermarks.
- Reports should include counterexamples that explain specific violations.
