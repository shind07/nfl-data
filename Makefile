IMAGE_NAME=scottyhind/nfl-data
IMAGE_TAG:=$(shell git rev-parse HEAD)
WORKDIR=opt/nfl

.PHONY: build
build:
	@echo building $(IMAGE_NAME) image...
	@docker build \
		--cache-from $(IMAGE_NAME):latest \
		-t $(IMAGE_NAME):latest \
		-t $(IMAGE_NAME):$(IMAGE_TAG) .

.PHONY: run
run:
	docker-compose run \
		--rm \
		-v $(shell PWD)/app:/$(WORKDIR)/app \
		app

.PHONY: notebook
notebook:
	WORKDIR=$(WORKDIR) \
	docker-compose run --rm \
		-p 8888:8888 \
		-v $(shell PWD)/data:/$(WORKDIR)/data \
		-v $(shell PWD)/app:/$(WORKDIR)/app \
		-v $(shell PWD)/notebooks:/$(WORKDIR)/notebooks \
		app jupyter notebook --ip=0.0.0.0 --allow-root notebooks

.PHONY: up
up:
	WORKDIR=$(WORKDIR) docker-compose up -d

.PHONY: down
down:
	docker-compose down


.PHONY: db-shell
db-shell:
	docker-compose run --rm app psql -U postgres --host postgres -d nfl -w

.PHONY: shell
shell:
	docker-compose run --rm \
		-v $(shell PWD)/alembic:/$(WORKDIR)/alembic \
		-v $(shell PWD)/data:/$(WORKDIR)/data \
		-v $(shell PWD)/app:/$(WORKDIR)/app \
		app bash

.PHONY: migrate
migrate:
	docker-compose run --rm \
		-v $(shell PWD)/alembic:/$(WORKDIR)/alembic \
		app alembic upgrade head

.PHONY: lint
lint:
	@flake8