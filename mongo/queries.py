from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import time
from datetime import datetime, timedelta
import uuid
from bson.codec_options import UuidRepresentation
import random


MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "techmarket_db"
NUM_RUNS = 10


def connect_to_mongodb():
    client = None
    try:
        client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000,
        )
        client.admin.command("ping")
        print("Conexão ao MongoDB estabelecida.")
        return client
    except ConnectionFailure as e:
        print(f"Erro ao conectar ao MongoDB: {e}")
        return None


def measure_execution_time(mongo_cursor_or_pipeline_result):
    start_time = time.time()
    results = list(mongo_cursor_or_pipeline_result)
    end_time = time.time()
    return (end_time - start_time) * 1000, results


def run_mongodb_queries():
    client = connect_to_mongodb()
    if not client:
        print("Não foi possível conectar ao MongoDB. Encerrando consultas.")
        return

    db = client[DB_NAME]

    try:

        print(
            "\n--- Executando Q1: Buscar cliente por email e listar seus últimos 3 pedidos ---"
        )

        sample_client = db.clientes.aggregate(
            [{"$sample": {"size": 1}}, {"$project": {"email": 1, "_id": 1}}]
        ).next()
        if sample_client:
            client_email = sample_client["email"]
            client_id = sample_client["_id"]
            q1_times = []
            for _ in range(NUM_RUNS):
                pipeline = [
                    {"$match": {"_id": client_id}},
                    {
                        "$lookup": {
                            "from": "pedidos",
                            "localField": "_id",
                            "foreignField": "id_cliente",
                            "as": "pedidos_do_cliente",
                        }
                    },
                    {"$unwind": "$pedidos_do_cliente"},
                    {"$sort": {"pedidos_do_cliente.data_pedido": -1}},
                    {"$limit": 3},
                    {
                        "$project": {
                            "nome_cliente": "$nome",
                            "email_cliente": "$email",
                            "pedido_id": "$pedidos_do_cliente._id",
                            "pedido_data": "$pedidos_do_cliente.data_pedido",
                            "pedido_status": "$pedidos_do_cliente.status",
                            "pedido_valor_total": "$pedidos_do_cliente.valor_total",
                        }
                    },
                ]

                time_taken, results = measure_execution_time(
                    db.clientes.aggregate(pipeline)
                )
                q1_times.append(time_taken)
            avg_time = sum(q1_times) / NUM_RUNS
            print(f"Média de tempo (Q1): {avg_time:.2f} ms")
            print(
                f"Exemplo de resultado (Q1 - cliente {client_email}): {results[0] if results else 'Nenhum pedido encontrado.'}"
            )
        else:
            print("Nenhum cliente encontrado para testar Q1.")

        print(
            "\n--- Executando Q2: Listar produtos de uma categoria ordenados por preço ---"
        )

        sample_product = db.produtos.aggregate(
            [{"$sample": {"size": 1}}, {"$project": {"categoria": 1}}]
        ).next()
        if sample_product:
            product_category = sample_product["categoria"]
            q2_times = []
            for _ in range(NUM_RUNS):
                query_filter = {"categoria": product_category}

                cursor = db.produtos.find(
                    query_filter,
                    {"nome": 1, "categoria": 1, "preco": 1, "estoque": 1, "_id": 0},
                ).sort("preco", 1)
                time_taken, results = measure_execution_time(cursor)
                q2_times.append(time_taken)
            avg_time = sum(q2_times) / NUM_RUNS
            print(f"Média de tempo (Q2): {avg_time:.2f} ms")
            print(
                f"Exemplo de resultado (Q2 - categoria {product_category}): {results[0] if results else 'Nenhum produto encontrado.'}"
            )
        else:
            print("Nenhuma categoria encontrada para testar Q2.")

        print(
            "\n--- Executando Q3: Listar pedidos de um cliente com status 'entregue' ---"
        )

        sample_client_q3 = db.clientes.aggregate(
            [{"$sample": {"size": 1}}, {"$project": {"_id": 1}}]
        ).next()
        if sample_client_q3:
            client_id_q3 = sample_client_q3["_id"]
            q3_times = []
            for _ in range(NUM_RUNS):
                query_filter = {"id_cliente": client_id_q3, "status": "entregue"}

                cursor = db.pedidos.find(
                    query_filter,
                    {"data_pedido": 1, "status": 1, "valor_total": 1, "_id": 1},
                ).sort("data_pedido", -1)
                time_taken, results = measure_execution_time(cursor)
                q3_times.append(time_taken)
            avg_time = sum(q3_times) / NUM_RUNS
            print(f"Média de tempo (Q3): {avg_time:.2f} ms")
            print(
                f"Exemplo de resultado (Q3 - cliente {client_id_q3}): {results[0] if results else 'Nenhum pedido entregue encontrado.'}"
            )
        else:
            print("Nenhum cliente encontrado para testar Q3.")

        print("\n--- Executando Q4: Obter os 5 produtos mais vendidos ---")
        q4_times = []
        for _ in range(NUM_RUNS):
            pipeline = [
                {"$unwind": "$itens"},
                {
                    "$group": {
                        "_id": "$itens.id_produto",
                        "total_vendido": {"$sum": "$itens.quantidade"},
                    }
                },
                {"$sort": {"total_vendido": -1}},
                {"$limit": 5},
                {
                    "$lookup": {
                        "from": "produtos",
                        "localField": "_id",
                        "foreignField": "_id",
                        "as": "produto_info",
                    }
                },
                {"$unwind": "$produto_info"},
                {
                    "$project": {
                        "nome_produto": "$produto_info.nome",
                        "categoria": "$produto_info.categoria",
                        "total_vendido": 1,
                        "_id": 0,
                    }
                },
            ]
            time_taken, results = measure_execution_time(db.pedidos.aggregate(pipeline))
            q4_times.append(time_taken)
        avg_time = sum(q4_times) / NUM_RUNS
        print(f"Média de tempo (Q4): {avg_time:.2f} ms")
        print(
            f"Exemplo de resultado (Q4): {results[0] if results else 'Nenhum produto vendido encontrado.'}"
        )

        print(
            "\n--- Executando Q5: Consultar pagamentos feitos via PIX no último mês ---"
        )

        sample_payment = db.pedidos.aggregate(
            [
                {"$match": {"pagamento.tipo": "pix"}},
                {"$sample": {"size": 1}},
                {"$project": {"pagamento.data_pagamento": 1}},
            ]
        ).next()
        if (
            sample_payment
            and "pagamento" in sample_payment
            and "data_pagamento" in sample_payment["pagamento"]
        ):
            reference_date = sample_payment["pagamento"]["data_pagamento"]

            start_date_q5 = reference_date.replace(
                day=1, hour=0, minute=0, second=0, microsecond=0
            )
            if reference_date.month == 12:
                end_date_q5 = reference_date.replace(
                    year=reference_date.year + 1,
                    month=1,
                    day=1,
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=0,
                ) - timedelta(microseconds=1)
            else:
                end_date_q5 = reference_date.replace(
                    month=reference_date.month + 1,
                    day=1,
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=0,
                ) - timedelta(microseconds=1)

            q5_times = []
            for _ in range(NUM_RUNS):
                query_filter = {
                    "pagamento.tipo": "pix",
                    "pagamento.data_pagamento": {
                        "$gte": start_date_q5,
                        "$lte": end_date_q5,
                    },
                }

                cursor = db.pedidos.find(
                    query_filter, {"pagamento": 1, "_id": 1, "id_cliente": 1}
                ).sort("pagamento.data_pagamento", -1)
                time_taken, results = measure_execution_time(cursor)
                q5_times.append(time_taken)
            avg_time = sum(q5_times) / NUM_RUNS
            print(f"Média de tempo (Q5): {avg_time:.2f} ms")
            print(
                f"Exemplo de resultado (Q5 - PIX no período {start_date_q5.strftime('%Y-%m')}): {results[0] if results else 'Nenhum pagamento PIX encontrado neste período.'}"
            )
        else:
            print("Nenhum pagamento PIX encontrado para testar Q5.")

        # --- Q6: Obter o valor total gasto por um cliente em pedidos em um período ---
        # This section is now correctly dedented
        print(
            "\n--- Executando Q6: Obter o valor total gasto por um cliente em pedidos em um período ---"
        )

        # Q6: Obter o valor total gasto por um cliente em pedidos em um período
        # Modificação: Selecionar um pedido aleatório para garantir que o cliente tem pedidos
        sample_order_for_q6 = db.pedidos.aggregate(
            [
                {"$sample": {"size": 1}},
                {"$project": {"id_cliente": 1, "data_pedido": 1}},
            ]
        ).next()

        if sample_order_for_q6:
            client_id_q6 = sample_order_for_q6["id_cliente"]
            reference_date_q6 = sample_order_for_q6["data_pedido"]

            # Define o período: 90 dias antes da data do pedido de referência até a data do pedido de referência
            end_date_q6 = reference_date_q6
            start_date_q6 = end_date_q6 - timedelta(days=90)

            q6_times = []
            for _ in range(NUM_RUNS):
                pipeline = [
                    {
                        "$match": {
                            "id_cliente": client_id_q6,
                            "data_pedido": {
                                "$gte": start_date_q6,
                                "$lte": end_date_q6,
                            },
                        }
                    },
                    {
                        "$group": {
                            "_id": "$id_cliente",
                            "total_gasto": {"$sum": "$valor_total"},
                        }
                    },
                ]
                time_taken, results = measure_execution_time(
                    db.pedidos.aggregate(pipeline)
                )
                q6_times.append(time_taken)

            avg_time = sum(q6_times) / NUM_RUNS
            print(f"Média de tempo (Q6): {avg_time:.2f} ms")
            print(
                f"Exemplo de resultado (Q6 - cliente {client_id_q6} no período {start_date_q6.strftime('%Y-%m-%d')} a {end_date_q6.strftime('%Y-%m-%d')}): {results[0] if results else 'Nenhum gasto encontrado para o cliente no período.'}"
            )
        else:
            print(
                "Nenhum pedido encontrado para testar Q6. Certifique-se de que há dados na coleção 'pedidos'."
            )

    except ConnectionFailure as e:
        print(f"Erro de conexão ao executar consultas no MongoDB: {e}")
    except Exception as e:
        print(f"Erro inesperado ao executar consultas no MongoDB: {e}")
    finally:
        client.close()
        print("\nConexão ao MongoDB fechada.")


if __name__ == "__main__":
    run_mongodb_queries()
