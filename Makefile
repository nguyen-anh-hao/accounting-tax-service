.DEFAULT_GOAL := help
SHELL := /bin/bash

ifneq (,$(wildcard .env))
include .env
export
endif

MVN ?= mvn
COMPOSE ?= docker compose
COMPOSE_FILE ?= deployments/docker/docker-compose.dev.yml
IMAGE ?= accounting-tax-service:local

.PHONY: help env deps deps-down run test lint build check docker clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

env: ## Create .env from .env.example when missing
	@test -f .env || cp .env.example .env

deps: ## Start Kafka and Schema Registry for local development
	$(COMPOSE) -f $(COMPOSE_FILE) up -d

deps-down: ## Stop and wipe the local development stack
	$(COMPOSE) -f $(COMPOSE_FILE) down -v

run: env ## Run the service locally
	$(MVN) -B spring-boot:run

test: ## Run unit tests
	$(MVN) -B test

lint: ## No linter configured; compile only
	$(MVN) -B -DskipTests compile

build: ## Package the jar
	$(MVN) -B -DskipTests package

check: env ## Compile, test and package
	$(MVN) -B verify

docker: ## Build the container image
	docker build -t $(IMAGE) .

clean: ## Remove Maven output
	$(MVN) -B clean
