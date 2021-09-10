#!/usr/bin/env bash
airflow db init
airflow users create -r Admin -u admin -e admin@natanascimento.com -f Natan -l Nascimento -p 123456
airflow webserver
echo "teste"