from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from bson.codec_options import CodecOptions, UuidRepresentation
from faker import Faker
from datetime import datetime
import random
import uuid
import time

# Configurações iniciais
fake = Faker("pt_BR")
NUM_CLIENTES = 20000
NUM_PRODUTOS = 5000
NUM_PEDIDOS = 30000

# Categorias e enums
CATEGORIAS = [
    "Eletrônicos",
    "Informática",
    "Games",
    "Celulares",
    "Periféricos",
    "Acessórios",
    "Eletrodomésticos",
    "Casa Inteligente",
]
TIPOS_PAGAMENTO = ["cartão", "pix", "boleto"]
STATUS_PAGAMENTO = ["aprovado", "pendente", "recusado"]
STATUS_PEDIDO = ["pendente", "processando", "entregue", "cancelado"]


def connect_to_mongodb():
    try:
        client = MongoClient(
            "mongodb://localhost:27017/", serverSelectionTimeoutMS=5000
        )
        client.admin.command("ping")
        return client
    except ConnectionFailure as e:
        print(f"Erro ao conectar ao MongoDB: {e}")
        return None


def gerar_clientes(n):
    clientes = []
    ids = []
    for _ in range(n):
        _id = uuid.uuid4()
        ids.append(_id)
        clientes.append(
            {
                "_id": _id,
                "nome": fake.name(),
                "email": fake.unique.email(),
                "telefone": fake.phone_number(),
                "data_cadastro": fake.date_time_between(
                    start_date="-2y", end_date="now"
                ),
                "cpf": fake.unique.cpf(),
            }
        )
    return clientes, ids


def gerar_produtos(n):
    produtos = []
    ids = []
    for _ in range(n):
        _id = uuid.uuid4()
        ids.append(_id)
        produtos.append(
            {
                "_id": _id,
                "nome": f"{fake.word().capitalize()} {fake.word()} {fake.word()}",
                "categoria": random.choice(CATEGORIAS),
                "preco": round(random.uniform(10.0, 5000.0), 2),
                "estoque": random.randint(0, 1000),
            }
        )
    return produtos, ids


def gerar_pedidos(n, client_ids, product_ids):
    pedidos = []
    for _ in range(n):
        order_id = uuid.uuid4()
        client_id = random.choice(client_ids)
        data_pedido = fake.date_time_between(start_date="-1y", end_date="now")
        status = random.choice(STATUS_PEDIDO)

        num_items = random.randint(1, 5)
        selected_ids = random.sample(product_ids, num_items)
        itens = []
        total = 0.0

        for pid in selected_ids:
            qtd = random.randint(1, 3)
            preco = round(random.uniform(10.0, 1000.0), 2)
            total += qtd * preco
            itens.append(
                {
                    "id_produto": pid,
                    "quantidade": qtd,
                    "preco_unitario": preco,
                }
            )

        pagamento = {
            "tipo": random.choice(TIPOS_PAGAMENTO),
            "status": random.choice(STATUS_PAGAMENTO),
            "data_pagamento": fake.date_time_between(start_date="-6m", end_date="now"),
        }

        pedidos.append(
            {
                "_id": order_id,
                "id_cliente": client_id,
                "data_pedido": data_pedido,
                "status": status,
                "itens": itens,
                "valor_total": round(total, 2),
                "pagamento": pagamento,
            }
        )

    return pedidos


def populate_mongodb():
    client = connect_to_mongodb()
    if not client:
        print("Conexão falhou. Encerrando.")
        return

    db = client.get_database(
        "techmarket_db",
        codec_options=CodecOptions(uuid_representation=UuidRepresentation.STANDARD),
    )
    start = time.time()

    try:
        # Clientes
        print(f"Gerando {NUM_CLIENTES} clientes...")
        clientes, client_ids = gerar_clientes(NUM_CLIENTES)
        db.clientes.insert_many(clientes)
        print("Clientes inseridos.")

        # Produtos
        print(f"Gerando {NUM_PRODUTOS} produtos...")
        produtos, product_ids = gerar_produtos(NUM_PRODUTOS)
        db.produtos.insert_many(produtos)
        print("Produtos inseridos.")

        # Pedidos
        print(f"Gerando {NUM_PEDIDOS} pedidos com pagamentos aninhados...")
        pedidos = gerar_pedidos(NUM_PEDIDOS, client_ids, product_ids)
        db.pedidos.insert_many(pedidos)
        print("Pedidos inseridos.")

        elapsed = time.time() - start
        print(f"\nPopulação concluída em {elapsed:.2f} segundos.")
    except Exception as e:
        print(f"Erro ao popular MongoDB: {e}")
    finally:
        client.close()


if __name__ == "__main__":
    populate_mongodb()
