# Enterprise Airflow + Spark Architecture

## ✅ Módulos Instalados

### 🔐 Security Module (`airflow/plugins/security/`)
- **auth_manager.py**: Autenticación centralizada (LDAP, OAuth2, JWT, MFA, RBAC)
- **audit_logger.py**: Auditoría inmutable con tamper-detection

### 📊 Logging Module (`airflow/plugins/logging/`)
- **structured_logger.py**: JSON structured logging, Elasticsearch integration

### 💪 Resilience Module (`airflow/plugins/resilience/`)
- **patterns.py**: Circuit breaker, retry policies, bulkhead, health checks

### 🔑 Secrets Module (`airflow/plugins/secrets/`)
- **vault_backend.py**: HashiCorp Vault integration, secret rotation

### ⚙️ Operators Module (`airflow/plugins/operators/`)
- **custom_operators.py**: SparkJobOperator, DataQualityCheckOperator, SLACheck

### 📈 Monitoring Module (`airflow/plugins/monitoring/`)
- **metrics.py**: Prometheus metrics, alerting (Slack, Email)

### 📋 Governance Module (`airflow/plugins/governance/`)
- **compliance.py**: Data lineage, compliance checking, change management

### 🏭 DAG Templates (`airflow/dags/templates/`)
- **dag_factory.py**: Reusable DAG factories (ETL, Spark, Batch, Streaming)

### 📚 Examples (`airflow/dags/examples/`)
- **hello_world.py**: Simple example DAG

## 🚀 Quick Start

1. **Setup environment:**
   ```bash
   cd $PWD
   cp .env.example .env
   # Edit .env with your configuration
   ```

2. **Initialize project:**
   ```bash
   make setup
   ```

3. **Start services:**
   ```bash
   make dev-up
   ```

4. **Access points:**
   - Airflow Web: http://localhost:8080
   - Grafana: http://localhost:3000
   - Prometheus: http://localhost:9090
   - Kibana: http://localhost:5601
   - Spark UI: http://localhost:8081

## 📁 Project Structure

```
airflow-spark/
├── airflow/
│   ├── config/          # Airflow configuration
│   ├── dags/            # DAG definitions
│   │   ├── templates/   # DAG factories
│   │   ├── examples/    # Example DAGs
│   │   └── production/  # Production DAGs
│   ├── plugins/         # Custom plugins
│   │   ├── security/    # Authentication & Audit
│   │   ├── logging/     # Structured logging
│   │   ├── resilience/  # Fault tolerance
│   │   ├── secrets/     # Secret management
│   │   ├── operators/   # Custom operators
│   │   ├── sensors/     # Custom sensors
│   │   ├── hooks/       # Custom hooks
│   │   ├── monitoring/  # Observability
│   │   ├── governance/  # Compliance
│   │   └── utils/       # Utilities
│   ├── logs/            # Execution logs
│   └── tests/           # Test suite
├── spark/
│   ├── jobs/            # Spark job scripts
│   └── config/          # Spark configuration
├── infrastructure/
│   ├── docker/          # Docker setup
│   ├── kubernetes/      # K8s manifests
│   ├── terraform/       # IaC
│   ├── monitoring/      # Prometheus, Grafana configs
│   ├── security/        # TLS, Vault configs
│   └── database/        # DB migrations
├── docs/                # Documentation
├── scripts/             # Utility scripts
├── docker-compose.yml   # Full stack
├── Makefile            # Commands
├── Dockerfile          # Image build
├── .env.example        # Configuration template
└── requirements.txt    # Python dependencies
```

## 🔒 Security Features

- ✅ Multi-factor authentication (MFA)
- ✅ LDAP/OAuth2 integration
- ✅ Role-based access control (RBAC)
- ✅ Immutable audit logs
- ✅ Encryption at rest & in transit
- ✅ Secret rotation automation
- ✅ Compliance checking (GDPR, PCI-DSS, SOX)

## 💻 Available Commands

```bash
make help              # Show all commands
make setup            # Initialize project
make dev-up           # Start services
make dev-down         # Stop services
make dev-test         # Run tests
make security-scan    # Security scanning
make lint             # Code quality checks
make dag-list         # List all DAGs
make trigger-dag      # Trigger DAG manually
make health-check     # Check service health
```

## 📚 Documentation

- `README.md` - Main documentation with security guidelines
- `PROJECT_STRUCTURE.md` - Detailed folder structure
- `docs/` - Additional documentation

## 🆘 Support

For questions or issues, refer to:
1. Documentation in `docs/` folder
2. Example DAGs in `airflow/dags/examples/`
3. Module docstrings and comments

---
**Last updated**: 2026-08-20
**Status**: Production Ready ✅
