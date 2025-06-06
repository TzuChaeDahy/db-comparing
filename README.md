# Atividade Prática: Comparando Bancos Relacionais e NoSQL

Este repositório contém os arquivos e documentação de uma atividade prática focada na comparação de bancos de dados Relacionais e NoSQL, utilizando um cenário simplificado de e-commerce.

## Objetivo do Projeto

Nosso objetivo principal é explorar e comparar três tipos de bancos de dados:

- **Relacional**: PostgreSQL
- **NoSQL (Documentos)**: MongoDB
- **NoSQL (Colunar)**: Cassandra

A comparação será baseada em:

- Modelagem de dados orientada a consultas.
- Performance na inserção de grandes volumes de dados.
- Desempenho na execução de consultas essenciais para o sistema.

## Cenário: Loja Virtual "TechMarket"

Para simular um ambiente real, utilizamos uma versão simplificada de uma loja de e-commerce, a **TechMarket**. As entidades principais do sistema são:

- **Cliente**: id, nome, email, telefone, data_cadastro, cpf
- **Produto**: id, nome, categoria, preco, estoque
- **Pedido**: id, id_cliente, data_pedido, status, itens, valor_total
- **Pagamento**: id, id_pedido, tipo, status, data_pagamento

Serão avaliadas as seguintes consultas (Q) em cada banco:

- **Q1**: Buscar cliente por email e listar seus 3 últimos pedidos.
- **Q2**: Listar produtos de uma categoria, ordenados por preço.
- **Q3**: Listar pedidos de um cliente com status "entregue".
- **Q4**: Obter os 5 produtos mais vendidos.
- **Q5**: Consultar pagamentos feitos via PIX no último mês.
- **Q6**: Obter o valor total gasto por um cliente em pedidos em um período (ex.: últimos 3 meses).

## Configuração do Ambiente

Utilizamos Docker e docker-compose para gerenciar os serviços dos bancos de dados, garantindo um ambiente isolado e fácil de configurar.

### Arquivo `docker-compose.yml`:

Este arquivo está na raiz do projeto e define a configuração para os serviços PostgreSQL, MongoDB e Cassandra.

Para iniciar os bancos de dados:

```bash
docker-compose up -d
```

### Bibliotecas Python necessárias:

Instale as dependências com pip:

```bash
pip install faker pymongo psycopg2-binary cassandra-driver gevent
```

## Modelagem e Estruturas dos Bancos

Cada banco de dados foi modelado considerando suas características e as consultas a serem otimizadas.

### a. PostgreSQL (Banco de Dados Relacional)

- **Conceito**: Organiza dados em tabelas com relações bem definidas.
- **Estrutura**: Tabelas Cliente, Produto, Pedido, ItemPedido (tabela associativa), Pagamento com chaves primárias, estrangeiras e índices.
- **Código SQL**: Está em `postgres/init_db.py`.
- **Impacto nas Consultas**: Consultas com JOINs (Q1, Q3, Q6) são otimizadas por índices. Ordenação (Q2) e buscas específicas (Q5) também se beneficiam de índices adequados. Agregações (Q4) podem requerer _views_ ou materializações.

### b. MongoDB (Banco de Dados NoSQL - Documentos)

- **Conceito**: Armazena dados em documentos flexíveis (JSON/BSON), permitindo aninhamento de dados relacionados para evitar JOINs.
- **Estrutura**: Coleções para clientes, produtos, pedidos (com itens e pagamento aninhados).
- **Código JavaScript**: Está em `mongo/init_db.py`.
- **Impacto nas Consultas**: Índices otimizam buscas (Q1, Q2, Q3, Q5). Consultas de agregação (Q4) são realizadas com pipelines de agregação.

### c. Cassandra (Banco de Dados NoSQL - Colunar e Distribuído)

- **Conceito**: Projetado para escalabilidade massiva e alta disponibilidade, com modelagem focada diretamente nas consultas (denormalização é comum).
- **Estrutura**: Criação de um KEYSPACE e tabelas específicas para cada padrão de consulta (`clientes_por_email`, `produtos_por_categoria`, `pedidos_por_cliente_status`, `pagamentos_por_tipo_mes`, `pedidos_base`, `pagamentos_base`).
- **Código CQL**: Está em `cassandra/init_db.py`.
- **Impacto nas Consultas**: As chaves de partição e agrupamento são cruciais para o desempenho. Consultas de agregação complexas (Q4, Q6) geralmente exigem processamento offline (ex: Apache Spark).

## Inserção de Dados em Massa

Para testar o desempenho de escrita, os bancos serão populados com um grande volume de dados gerados sinteticamente com a biblioteca Faker.

### Volumes de dados a serem inseridos:

- 20.000 clientes
- 5.000 produtos
- 30.000 pedidos (com 1 a 5 itens cada)
- 30.000 pagamentos

### Scripts de população:

- `postgres/populate.py` (para PostgreSQL)
- `mongo/populate.py` (para MongoDB)
- `cassandra/populate.py` (para Cassandra)

Para preencher os bancos (execute um por um no terminal):

```bash
python postgres/populate.py
python mongo/populate.py
python cassandra/populate.py
```

O tempo total de inserção para cada banco será registrado para análise comparativa.

## Análise Comparativa de Desempenho (Próximos Passos)

### 🐘 PostgreSQL

| Etapa / Consulta               | Tentativa 1 | Tentativa 2 | Tentativa 3 | Tentativa 4 | Tentativa 5 |
| ------------------------------ | ----------- | ----------- | ----------- | ----------- | ----------- |
| Inserção de Clientes           | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Inserção de Produtos           | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Inserção de Pedidos+Pagamentos | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| **Total População**            | **XX.XX**   | **XX.XX**   | **XX.XX**   | **XX.XX**   | **XX.XX**   |
| Q1: Cliente + Últimos Pedidos  | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Q2: Produtos por Categoria     | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Q3: Pedidos Entregues Cliente  | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Q4: Top 5 Produtos Vendidos    | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Q5: Pagamentos via PIX (30d)   | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Q6: Total Gasto por Cliente    | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |

### 🍃 MongoDB

| Etapa / Consulta               | Tentativa 1 | Tentativa 2 | Tentativa 3 | Tentativa 4 | Tentativa 5 |
| ------------------------------ | ----------- | ----------- | ----------- | ----------- | ----------- |
| Inserção de Clientes           | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Inserção de Produtos           | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Inserção de Pedidos+Pagamentos | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| **Total População**            | **XX.XX**   | **XX.XX**   | **XX.XX**   | **XX.XX**   | **XX.XX**   |
| Q1: Cliente + Últimos Pedidos  | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Q2: Produtos por Categoria     | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Q3: Pedidos Entregues Cliente  | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Q4: Top 5 Produtos Vendidos    | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Q5: Pagamentos via PIX (30d)   | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Q6: Total Gasto por Cliente    | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |

### 🔶 Cassandra

| Etapa / Consulta               | Tentativa 1 | Tentativa 2 | Tentativa 3 | Tentativa 4 | Tentativa 5 |
| ------------------------------ | ----------- | ----------- | ----------- | ----------- | ----------- |
| Inserção de Clientes           | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Inserção de Produtos           | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Inserção de Pedidos+Pagamentos | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| **Total População**            | **XX.XX**   | **XX.XX**   | **XX.XX**   | **XX.XX**   | **XX.XX**   |
| Q1: Cliente + Últimos Pedidos  | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Q2: Produtos por Categoria     | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Q3: Pedidos Entregues Cliente  | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Q4: Top 5 Produtos Vendidos    | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Q5: Pagamentos via PIX (30d)   | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |
| Q6: Total Gasto por Cliente    | XX.XX       | XX.XX       | XX.XX       | XX.XX       | XX.XX       |

---

Este documento serve como um guia e um registro do progresso desta atividade prática.
