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
	@docker run \
		--rm \
		-v $(shell PWD)/data:/$(WORKDIR)/data $(IMAGE_NAME)

.PHONY: pipeline
pipeline:
	@docker run \
		--rm \
		-v $(shell PWD)/data:/$(WORKDIR)/data \
		$(IMAGE_NAME) python3 -m app

.PHONY: notebook
notebook:
	@docker run \
		--rm \
		-p 8888:8888 \
		-v $(shell PWD)/data:/$(WORKDIR)/data \
		-v $(shell PWD)/app:/$(WORKDIR)/app \
		-v $(shell PWD)/notebooks:/$(WORKDIR)/notebooks \
		$(IMAGE_NAME) jupyter notebook --ip=0.0.0.0 --allow-root .

.PHONY: up
up:
	WORKDIR=$(WORKDIR) docker-compose up -d

.PHONY: down
down:
	docker-compose down


.PHONY: db-shell
db-shell: 
	docker-compose run app psql -U postgres --host postgres

.PHONY: shell
shell:
	docker-compose run \
		-v $(shell PWD)/alembic:/$(WORKDIR)/alembic \
		-v $(shell PWD)/data:/$(WORKDIR)/data \
		app bash

.PHONY: lint
lint:
	@flake8