.PHONY: proto test test-integration test-e2e

TEST_COMPOSE_FILE=docker-compose.test.yml
E2E_COMPOSE_FILE=docker-compose.e2e.yml

proto:
	@export PATH="$$PATH:$$(go env GOPATH)/bin"; \
	go generate ./proto

test:
	go test ./...

test-integration:
	@docker compose -f $(TEST_COMPOSE_FILE) up -d --wait
	@IPFSNIFFER_IT_OPENSEARCH_URL=http://127.0.0.1:9200 go test -tags=integration ./... -run TestIntegration -count=1
	@docker compose -f $(TEST_COMPOSE_FILE) down -v

test-e2e:
	@docker compose -f $(E2E_COMPOSE_FILE) up -d --wait
	@IPFSNIFFER_E2E_OPENSEARCH_URL=http://127.0.0.1:9200 \
	IPFSNIFFER_E2E_TIKA_URL=http://127.0.0.1:9998 \
	IPFSNIFFER_E2E_NATS_URL=nats://127.0.0.1:4222 \
	IPFSNIFFER_E2E_REDIS_ADDR=127.0.0.1:6379 \
	go test -tags=e2e ./... -run TestE2E -count=1
	@docker compose -f $(E2E_COMPOSE_FILE) down -v
