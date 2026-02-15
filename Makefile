# ----------------------------------
# Config
# ----------------------------------

POSTGRES_DIR := Postgress_Docker
POSTGRES_IMAGE := custom-postgres
POSTGRES_TAG := 1.0
E2E_TEST := dbt-model-diff/tests/integration/test_postgres_e2e.py
# ----------------------------------
# Phony targets
# ----------------------------------

.PHONY: \
	postgres-build \
	postgres-up \
	postgres-down \
	postgres-logs \
	postgres-ps \
	postgres-clean \
	postgres-reset \
	e2e-test


# Run E2E integration test
e2e-test:
	pytest $(E2E_TEST)


# ----------------------------------
# Build Postgres image
# ----------------------------------

postgres-build:
	docker build $(POSTGRES_DIR) -t $(POSTGRES_IMAGE):$(POSTGRES_TAG)

# ----------------------------------
# Start Postgres container
# ----------------------------------

postgres-up:
	docker compose -f $(POSTGRES_DIR)/docker-compose.yaml up -d

# ----------------------------------
# Stop Postgres container
# ----------------------------------

postgres-down:
	docker compose -f $(POSTGRES_DIR)/docker-compose.yaml down

# ----------------------------------
# Show running containers
# ----------------------------------

postgres-ps:
	docker compose -f $(POSTGRES_DIR)/docker-compose.yaml ps

# ----------------------------------
# Follow Postgres logs
# ----------------------------------

postgres-logs:
	docker compose -f $(POSTGRES_DIR)/docker-compose.yaml logs -f

# ----------------------------------
# Stop containers + remove volumes
# ----------------------------------

postgres-clean:
	docker compose -f $(POSTGRES_DIR)/docker-compose.yaml down -v

# ----------------------------------
# Full reset (containers + volumes + image)
# ----------------------------------

postgres-reset: postgres-clean
	docker rmi $(POSTGRES_IMAGE):$(POSTGRES_TAG) || true