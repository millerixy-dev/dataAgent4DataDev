# A-line local dev environment helpers.
#
# All targets run from repo root. Compose files live in compose/.

COMPOSE_DIR    := compose
COMPOSE_FILE   := $(COMPOSE_DIR)/docker-compose.yml
COMPOSE_DS     := $(COMPOSE_DIR)/docker-compose.ds.yml
ENV_FILE       := $(COMPOSE_DIR)/.env

.PHONY: help up up-ds up-all down down-ds down-all logs logs-ds ps clean wipe-warehouse hms-shell test-compose

help:
	@echo "Targets:"
	@echo "  up            Start HMS + Postgres (foreground:false). Wait for healthy."
	@echo "  up-ds         Start DolphinScheduler standalone. Requires 'up' first."
	@echo "  up-all        up + up-ds"
	@echo "  down          Stop HMS stack (keeps volumes)."
	@echo "  down-ds       Stop DS."
	@echo "  down-all      down + down-ds"
	@echo "  logs / logs-ds  Tail logs (Ctrl-C to leave)."
	@echo "  ps            List compose-managed containers."
	@echo "  clean         down + drop volumes (pg-data, ds-data, ds-logs)."
	@echo "  wipe-warehouse  rm -rf compose/warehouse/* (Hive table data)."
	@echo "  hms-shell     beeline-style psql into the metastore DB."
	@echo "  test-compose  Run pytest suite filtered to compose-driven tests."

$(ENV_FILE):
	@if [ ! -f "$(ENV_FILE)" ]; then \
		echo "[Makefile] creating $(ENV_FILE) from .env.example"; \
		cp $(COMPOSE_DIR)/.env.example $(ENV_FILE); \
	fi

up: $(ENV_FILE)
	docker compose --env-file $(ENV_FILE) -f $(COMPOSE_FILE) up -d --wait

up-ds: $(ENV_FILE)
	docker compose --env-file $(ENV_FILE) -f $(COMPOSE_DS) up -d

up-all: up up-ds

down:
	docker compose -f $(COMPOSE_FILE) down

down-ds:
	docker compose -f $(COMPOSE_DS) down

down-all: down-ds down

logs:
	docker compose -f $(COMPOSE_FILE) logs -f --tail=200

logs-ds:
	docker compose -f $(COMPOSE_DS) logs -f --tail=200

ps:
	docker compose -f $(COMPOSE_FILE) ps
	@echo
	docker compose -f $(COMPOSE_DS) ps 2>/dev/null || true

clean:
	docker compose -f $(COMPOSE_DS) down -v 2>/dev/null || true
	docker compose -f $(COMPOSE_FILE) down -v

wipe-warehouse:
	rm -rf $(COMPOSE_DIR)/warehouse/* $(COMPOSE_DIR)/warehouse/.??* 2>/dev/null || true
	@echo "[Makefile] $(COMPOSE_DIR)/warehouse cleared."

hms-shell:
	docker exec -it dataagent-postgres psql -U hive -d metastore

test-compose:
	uv run pytest -m compose -v
