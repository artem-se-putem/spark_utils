.PHONY: up down restart clean shell logs

up:
	docker-compose up

up-d:
	docker-compose up -d

down:
	docker-compose down

restart:
	docker-compose down
	docker-compose up

clean:
	docker-compose down -v
	docker container prune -f

shell:
	docker exec -it jupyter-spark bash

logs:
	docker-compose logs -f

# Запуск с автоматической очисткой
start:
	@echo "Starting Jupyter Spark..."
	docker rm -f jupyter-spark 2>/dev/null || true
	docker-compose up

# Запуск в фоне
start-d:
	@echo "Starting Jupyter Spark in background..."
	docker rm -f jupyter-spark 2>/dev/null || true
	docker-compose up -d
	@echo "Jupyter is running at http://localhost:8888"
	@echo "Password: mysecretpassword"