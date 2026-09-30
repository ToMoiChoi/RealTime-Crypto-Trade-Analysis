# ============================================================
# Makefile - Binance Crypto Streaming Pipeline
# ============================================================
# Requirements: Python 3.10+, Docker Desktop running
# First run: make install
#
# Recommended workflow:
#   make install -> make start-kafka -> make setup-pg -> make seed-pg
#   -> make run-live (terminal 1) -> make run-spark (terminal 2)
#   -> make check-db (verify)
# ============================================================

.PHONY: help install setup-bq seed-bq setup-pg seed-pg start-kafka stop-kafka \
        run-live run-spark run-spark-docker reconcile check-db upload-bq \
        evaluate sensitivity inject-anomaly retry-dlq reset-spark clean logs

# -- Colors --------------------------------------------------------
GREEN  := \033[0;32m
YELLOW := \033[1;33m
CYAN   := \033[0;36m
RESET  := \033[0m

# -- Default target ------------------------------------------------
help:
	@echo ""
	@echo "$(CYAN)+======================================================+$(RESET)"
	@echo "$(CYAN)|    Binance Crypto Streaming Pipeline                 |$(RESET)"
	@echo "$(CYAN)+======================================================+$(RESET)"
	@echo ""
	@echo "$(YELLOW)Setup & Dependencies$(RESET)"
	@echo "  make install         Install Python dependencies"
	@echo "  make setup-pg        Create PostgreSQL Star Schema"
	@echo "  make seed-pg         Seed dimension tables to PostgreSQL"
	@echo "  make setup-bq        Create BigQuery dataset and tables"
	@echo "  make seed-bq         Seed dimension tables directly to BigQuery"
	@echo ""
	@echo "$(YELLOW)Infrastructure$(RESET)"
	@echo "  make start-kafka     docker-compose up (Kafka + Zookeeper + Postgres)"
	@echo "  make stop-kafka      docker-compose down"
	@echo "  make logs            Tail Kafka logs"
	@echo ""
	@echo "$(YELLOW)Pipeline Execution$(RESET)"
	@echo "  make run-live        Run live producer (Binance WebSocket -> Kafka)"
	@echo "  make run-spark       Run Spark Structured Streaming locally (dual sink)"
	@echo "  make run-spark-docker Run Spark in Docker container"
	@echo ""
	@echo "$(YELLOW)Testing & Benchmarking$(RESET)"
	@echo "  make inject-anomaly  Interactive synthetic anomaly injector (Kafka)"
	@echo "  make evaluate        Benchmark anomaly detection metrics (Precision/Recall)"
	@echo "  make sensitivity     Run threshold sensitivity analysis (Z-Score & Wash)"
	@echo "  make retry-dlq       Scan and retry failed BigQuery DLQ Parquet files"
	@echo ""
	@echo "$(YELLOW)Verification & Sync$(RESET)"
	@echo "  make check-db        Query live overview from Postgres DW"
	@echo "  make reconcile       Compare Kafka produced msgs vs BigQuery rows"
	@echo "  make upload-bq       Sync Postgres data offline to BigQuery"
	@echo ""
	@echo "$(YELLOW)Maintenance & Reset$(RESET)"
	@echo "  make reset-spark     Hard reset Spark checkpoints and truncate DW fact"
	@echo "  make clean           Remove __pycache__ and temporary buffers"
	@echo ""

# -- Install dependencies ------------------------------------------
install:
	@echo "$(GREEN)Installing Python dependencies...$(RESET)"
	pip install -r requirements.txt

# -- BigQuery schema setup -----------------------------------------
setup-bq:
	@echo "$(GREEN)Creating BigQuery dataset and tables...$(RESET)"
	python -m warehouse.bigquery_schema

# -- PostgreSQL schema setup ----------------------------------------
setup-pg:
	@echo "$(GREEN)Creating PostgreSQL Star Schema...$(RESET)"
	python -m warehouse.postgres_schema

# -- Seed BigQuery dimension tables (direct, no PG) ------------------
seed-bq:
	@echo "$(GREEN)Seeding dimension tables directly to BigQuery...$(RESET)"
	python -m warehouse.seed_dimensions_bq

# -- Seed PostgreSQL dimension tables --------------------------------
seed-pg:
	@echo "$(GREEN)Seeding PostgreSQL dimension tables...$(RESET)"
	python -m warehouse.seed_dimensions_pg

# -- Kafka infra -----------------------------------------------------
start-kafka:
	@echo "$(GREEN)Starting Kafka + Zookeeper + Postgres...$(RESET)"
	docker-compose up -d zookeeper kafka postgres
	@echo "$(YELLOW)Waiting 10s for services to be ready...$(RESET)"
	python -c "import time; time.sleep(10)"
	@echo "$(GREEN)Infrastructure is up.$(RESET)"

stop-kafka:
	@echo "$(YELLOW)Stopping all containers...$(RESET)"
	docker-compose down

logs:
	docker-compose logs -f kafka

# -- Live Producer (Binance WebSocket) --------------------------------
run-live:
	@echo "$(GREEN)Starting Live producer (Binance WebSocket)...$(RESET)"
	python -m producer.live_producer

# -- Spark (local mode) -----------------------------------------------
run-spark:
	@echo "$(GREEN)Starting Spark Processor (local)...$(RESET)"
	python processor/spark_processor.py

# -- Spark (Docker) ----------------------------------------------------
run-spark-docker:
	@echo "$(GREEN)Building and running Spark in Docker...$(RESET)"
	docker-compose up --build spark-processor

# -- Testing & Benchmarking -------------------------------------------
inject-anomaly:
	@echo "$(GREEN)Running interactive anomaly injection tool...$(RESET)"
	python scripts/inject_anomalies.py

evaluate:
	@echo "$(GREEN)Running anomaly detection evaluation benchmark...$(RESET)"
	python scripts/evaluate_anomalies.py

sensitivity:
	@echo "$(GREEN)Running anomaly threshold sensitivity analysis...$(RESET)"
	python scripts/sensitivity_analysis.py

retry-dlq:
	@echo "$(GREEN)Running DLQ self-healing retry script for BigQuery...$(RESET)"
	python scripts/retry_dlq_to_bq.py

# -- Verification -----------------------------------------------------
reconcile:
	@echo "$(GREEN)Running reconciliation check...$(RESET)"
	python -m warehouse.bq_reconcile

check-db:
	@echo "$(GREEN)Querying Postgres Data Warehouse...$(RESET)"
	python scripts/check_pg_data.py

upload-bq:
	@echo "$(GREEN)Syncing Postgres data to BigQuery...$(RESET)"
	python scripts/pg_to_bq_sync.py

# -- Maintenance & Reset ----------------------------------------------
reset-spark:
	@echo "$(YELLOW)Hard resetting Spark checkpoints and Postgres fact...$(RESET)"
	python scripts/hard_reset_spark.py

clean:
	@echo "$(YELLOW)Cleaning up temporary files...$(RESET)"
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	rm -rf /tmp/spark_checkpoint* 2>/dev/null || true
	rm -rf /tmp/bq_backup 2>/dev/null || true
	@echo "$(GREEN)Done.$(RESET)"
