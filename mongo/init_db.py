from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import time

def create_indexes_mongodb(client):
    db = client.techmarket_db

    try:

        db.clientes.create_index([("email", 1)], unique=True)
        print("Índice 'email' na coleção 'clientes' criado ou já existente.")

        db.produtos.create_index([("categoria", 1)])
        db.produtos.create_index([("preco", 1)])
        db.produtos.create_index([("categoria", 1), ("preco", 1)])
        print(
            "Índices 'categoria', 'preco' e composto em 'produtos' criados ou já existentes."
        )

        db.pedidos.create_index([("id_cliente", 1)])
        db.pedidos.create_index([("id_cliente", 1), ("status", 1)])
        db.pedidos.create_index([("id_cliente", 1), ("data_pedido", 1)])
        db.pedidos.create_index(
            [("pagamento.tipo", 1), ("pagamento.data_pagamento", 1)]
        )
        print("Índices em 'pedidos' criados ou já existentes.")

        print("Todos os índices do MongoDB criados com sucesso!")

    except ConnectionFailure as e:
        print(f"Erro de conexão ao criar índices no MongoDB: {e}")
    except Exception as e:
        print(f"Erro inesperado ao criar índices no MongoDB: {e}")


def connect_to_mongodb():
    client = None
    try:
        client = MongoClient(
            "mongodb://localhost:27017/", serverSelectionTimeoutMS=5000
        )
        client.admin.command("ping")
        print("Conexão ao MongoDB estabelecida com sucesso!")
        return client
    except ConnectionFailure as e:
        print(f"Erro ao conectar ao MongoDB: {e}")
        return None


if __name__ == "__main__":
    max_retries = 10
    retry_delay = 5

    for i in range(max_retries):
        print(f"Tentando conectar ao MongoDB... Tentativa {i + 1}/{max_retries}")
        client = connect_to_mongodb()
        if client:
            create_indexes_mongodb(client)
            client.close()
            print("Conexão ao MongoDB fechada.")
            break
        else:
            time.sleep(retry_delay)
    else:
        print(
            "Não foi possível conectar ao MongoDB após várias tentativas. Verifique se o Docker está rodando."
        )
