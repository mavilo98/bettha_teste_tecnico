
# Superhero Data Challenge 🦸‍♂️🦸‍♀️

## Descrição do Projeto

Este projeto foi desenvolvido como parte do desafio para a vaga de Engenheiro de Dados Pleno na Bettha. O objetivo do desafio foi integrar e deduplicar bases de dados de super-heróis de duas ligas (Marvel e DC), criar um esquema de banco de dados analítico e implementar a solução utilizando Python. A base analítica gerada é projetada para atender as necessidades dos times de negócio em termos de visualização de dashboards e exploração de iniciativas de Machine Learning, otimizando a alocação de heróis em novas missões.

## Racional da Solução

A solução proposta foi dividida em três partes principais:

1. **Integração e Deduplicação dos Dados:**
   - Os dados fornecidos em formato CSV foram integrados e deduplicados utilizando funções desenvolvidas em Python. Foram realizadas verificações para garantir que os registros duplicados fossem removidos com base em identificadores chave como `hero_id` e `mission_id`.

2. **Proposta e Implementação do Esquema Analítico:**
   - Um esquema analítico foi projetado considerando as necessidades de análise para a otimização de missões e alocação de heróis. O esquema foi implementado em tabelas dimensionais (dim_heroes, dim_villains, etc.) e uma tabela fato (fato_missions).
   - As tabelas foram salvas em formato Parquet, garantindo compatibilidade com Google BigQuery e otimizando o armazenamento e o desempenho das consultas.

3. **Análises e Visualizações:**
   - Foram realizadas algumas análises exploratórias para extrair insights dos dados, como a distribuição dos heróis por gênero e a duração média das missões.

## Estrutura do Repositório

O projeto está organizado da seguinte maneira:

```plaintext
├── data
│   ├── landing            # Dados brutos fornecidos
│   ├── raw                # Dados integrados e deduplicados
│   ├── trusted            # Dados com o schema estabelecido aplicado
│   └── analytics          # Tabelas dim e analíticas geradas em formato Parquet para o BigQuery
├── logs                   # Arquivos de log de execução
├── notebooks              # Notebooks Jupyter com algumas visualizações
├── src
│   ├── etl.py             # Script principal de ETL
│   ├── schema.py          # Screma proposto para a modelagem dos dados
│   └── utils.py           # Classe utilitária
└── README.md              # Este arquivo
```

## Instruções para Reproduzir

### 1. Pré-requisitos

-**Limpeza de Dados**
- A atual estrutura já possui os dados processados. Caso queira rodar a pipeline, limpe os dados em data/analytics, data/raw e data/trusted. 

- **Python 3.8+** deve estar instalado.
- Instale as dependências do projeto listadas em `requirements.txt` utilizando o seguinte comando:
  
  ```bash
  pip install -r requirements.txt
  ```

### 2. Executando o Pipeline de ETL

1. **Dados:**
   - Os dados fornecidos já estão em `data/landing`.

2. **Executar o Script de ETL:**
   - Navegue até a pasta `src` e execute o script `etl.py` para realizar a integração, deduplicação, e salvar as tabelas analíticas em formato Parquet:
  
  ```bash
  python etl.py
  ```

   - O script também gera logs que podem ser encontrados na pasta `logs`.

### 3. Visualizando as Análises

- Abra o notebook Jupyter localizado na pasta `notebooks` para visualizar as análises realizadas. As análises incluem gráficos gerados utilizando Matplotlib e fornecem insights sobre a distribuição de heróis, sucesso de missões, entre outros aspectos.

### 4. Carregando os Dados no Google BigQuery

- As tabelas em formato Parquet podem ser carregadas no Google BigQuery diretamente para análise adicional. O esquema foi projetado para ser facilmente carregado e consultado.

## Considerações Finais

Este projeto demonstra o processo completo de ETL, desde a ingestão de dados brutos até a geração de tabelas analíticas prontas para análise de negócios. Além disso, inclui exemplos de visualizações e insights úteis para a tomada de decisões estratégicas.

Sinta-se à vontade para explorar o código e as análises, e entre em contato caso tenha alguma dúvida ou sugestão!
