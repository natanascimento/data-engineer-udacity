# Big Data for Financial Market

Esse projeto foi desenvolvido para o captone do data engineer nanodegree

## Architecture 
![Pipeline Architecture](./docs/architecture.drawio.png)

## Overview

O projeto tem intuito em realizar a coleta, transformação e análise dos dados referente ao mercado financeiro, mais precisamente, os dados coletados são referentes ao indice do S&P500. Os dados processados podem ser utilizados para realizar compra de novos ativos, rebalanceamento de carteira, análise fundamentalista, análise de risco e muito mais. 

## Resources Used

- AlphaVantage API;
- Apache Airflow;
- Apache Spark;
- Pandas;
- AWS EC2;
- AWS S3;
- Docker;

## Data Lake Layers

- Bronze 
    - Bucket para armazenar dados raw;

- Silver:
    - Bucket para armazenar dados filtrados;

- Gold:
    - Bucket para armazenar dados alinhados com a camada de negócio;


## Data Pipelines

- PIPE_DATALAKE_SETUP
    - Pipeline responsável por criar o ambiente do Datalake e publicas todas as empresas do S&P500.

- PIPE_FINANCE_DATA_EXTRACTOR
    - Pipeline responsável por realizar a coleta dos dados no AlphaVantage API e realizar a ingestão dos dados na camada bronze (raw data).

- PIPE_FINANCE_DATA_PROCESSING
    - Pipeline responsável por coletar os dados da bronze layer e normalizar os dados para forma tabular, ao final o mesmo irá escrever os dados na camada silver.

- PIPE_FINANCE_DATA_QUALITY_ASSURANCE
    - Pipeline responsável por avaliar a qualidade de dados nos buckets do data lake.

- PIPE_FINANCE_GOLD_PROCESSING
    - Pipeline responsável por realizar a criação da camada gold e criar as camadas dimensão e fatos que serão utilizadas no negocio. 

## Pipeline Workflow Overview
