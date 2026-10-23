# Optional Azure deployment — design guide, no resources provisioned

Use AKS for the Helm-packaged applications, Azure Container Registry for built images,
Key Vault plus workload identity for secrets, and Azure Monitor/managed Prometheus/Grafana
if replacing local observability. Cost, region, capacity and retention need explicit review.
No cloud credentials, subscription, Terraform run or paid resource creation is required here.

The current chart is a single-node SQLite demonstration. A production deployment needs a
network database adapter (for example Azure Database for PostgreSQL), migrations and tested
concurrency before scaling API/worker replicas. A separate persistent JDBC backend for the
REST catalog can use a dedicated schema/database in the same managed PostgreSQL service.
This database service is needed to replace local-disk metadata/operational state, not to add
an unrelated system. The Python SQL repository migration has not been implemented.

For analytical objects, use ADLS Gen2 with a compatible PyIceberg FileIO and Iceberg Azure
FileIO/Spark bundle, or retain an explicitly supported S3-compatible service. Replacing an
S3 URL with a Blob URL alone is insufficient. Configure abfss paths, storage identity and
permissions, and rerun Python-write/Spark-read and snapshot recovery tests. This repository's
executed integration uses REST plus S3-compatible RustFS, not an Azure storage account.

For streaming, choose a managed Kafka service available in the selected Azure region, or
evaluate Event Hubs' Kafka endpoint. Validate producer idempotence, consumer groups, broker
metadata, API versions, topic lifecycle and restart semantics before substituting endpoints.
Do not assume Event Hubs is operationally identical to a Kafka broker.

Suggested deployment sequence:

1. Review capacity/cost and security requirements; obtain explicit deployment authorization.
2. Build, scan, tag and push immutable application images to your registry.
3. Provision chosen data services, network policies/private endpoints, backup and recovery.
4. Create workload identities and external secret references; never place credentials in values.yaml.
5. Deploy application Helm manifests, with real database and FileIO adapters after validation.
6. Run known-fixture smoke tests and model/schema compatibility checks before admitting traffic.
7. Calibrate each building, validate sensor units/provenance, monitor drift by season, and use
   independent safety interlocks, human approval and rollback before any future operational control.

This application remains simulation-only even when deployed in Azure. Training on injected
faults or reporting simulated savings does not establish field performance.

Official references: [AKS](https://learn.microsoft.com/en-us/azure/aks/what-is-aks),
[workload identity](https://learn.microsoft.com/en-us/azure/aks/workload-identity-overview),
[Iceberg FileIO configuration](https://py.iceberg.apache.org/configuration/),
[Event Hubs Kafka](https://learn.microsoft.com/en-us/azure/event-hubs/azure-event-hubs-kafka-overview).
