SHELL := /bin/bash

.PHONY: help setup run
.DEFAULT: help

help:
	@echo "make air"
	@echo "          Setup and run airflow container"
	@echo "----------"
	@echo "make setup"
	@echo "          Settings configure the virtual environments and install the dependencies"
	@echo "----------"
	@echo "make run"
	@echo "         Run project"
	@echo "----------"
	@echo "make test"
	@echo "         Testing project"
	@echo "----------"

air:
	docker compose up -d

clean:
	docker compose down

setup:
	python3 -m venv .venv
	mkdir airflow/logs

run:
	python3 main.py

test:
	clear && pytest -vv
