.PHONY: build test up down logs smoke benchmark clean

build:
	mvn -B -DskipTests package

test:
	mvn -B test

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f --tail=150

smoke:
	./scripts/smoke-test.sh

benchmark:
	./scripts/benchmark.sh

clean:
	mvn -B clean
