import psycopg2
from psycopg2 import OperationalError
from faker import Faker
import random
import time
from datetime import datetime, timedelta
import uuid

fake = Faker("pt_BR")

NUM_CLIENTES = 20000
NUM_PRODUTOS = 5000
NUM_PEDIDOS = 30000
NUM_PAGAMENTOS = 30000


def connect_to_postgres():
    """Conecta ao banco de dados PostgreSQL e retorna o objeto de conexão."""
    conn = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="mysecretpassword",
            port="5432",
        )
        return conn
    except OperationalError as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None


def populate_postgres():
    conn = connect_to_postgres()
    if not conn:
        print("Não foi possível conectar ao PostgreSQL. Encerrando população.")
        return

    cursor = conn.cursor()
    start_time = time.time()

    try:
        # --- 1. Popular Clientes ---
        print(f"Populando {NUM_CLIENTES} clientes no PostgreSQL...")
        client_ids = []
        for _ in range(NUM_CLIENTES):
            client_id = uuid.uuid4()
            client_ids.append(client_id)
            cursor.execute(
                """
                INSERT INTO Cliente (id, nome, email, telefone, data_cadastro, cpf)
                VALUES (%s, %s, %s, %s, %s, %s)
            """,
                (
                    str(client_id),  # Convertendo UUID para string
                    fake.name(),
                    fake.unique.email(),
                    fake.phone_number(),
                    fake.date_time_between(start_date="-2y", end_date="now"),
                    fake.unique.cpf(),
                ),
            )
        conn.commit()
        print("Clientes inseridos.")

        # --- 2. Popular Produtos ---
        print(f"Populando {NUM_PRODUTOS} produtos no PostgreSQL...")
        product_ids = []
        categories = [
            "Eletrônicos",
            "Informática",
            "Games",
            "Celulares",
            "Periféricos",
            "Acessórios",
            "Eletrodomésticos",
            "Casa Inteligente",
        ]
        for _ in range(NUM_PRODUTOS):
            product_id = uuid.uuid4()
            product_ids.append(product_id)
            cursor.execute(
                """
                INSERT INTO Produto (id, nome, categoria, preco, estoque)
                VALUES (%s, %s, %s, %s, %s)
            """,
                (
                    str(product_id),  # Convertendo UUID para string
                    fake.word().capitalize() + " " + fake.word() + " " + fake.word(),
                    random.choice(categories),
                    round(random.uniform(10.0, 5000.0), 2),
                    random.randint(0, 1000),
                ),
            )
        conn.commit()
        print("Produtos inseridos.")

        # --- 3. Popular Pedidos e ItemPedido ---
        print(f"Populando {NUM_PEDIDOS} pedidos e seus itens no PostgreSQL...")
        order_ids = []
        for _ in range(NUM_PEDIDOS):
            order_id = uuid.uuid4()
            order_ids.append(order_id)
            client_id = random.choice(client_ids)
            order_date = fake.date_time_between(start_date="-1y", end_date="now")
            status_options = ["pendente", "processando", "entregue", "cancelado"]
            status = random.choice(status_options)

            num_items = random.randint(1, 5)
            items_for_order = []
            valor_total = 0

            # Seleciona produtos aleatórios, garantindo que não exceda o número disponível
            selected_product_ids = random.sample(
                product_ids, min(num_items, len(product_ids))
            )

            for prod_id in selected_product_ids:
                quantity = random.randint(1, 3)
                price_unit = round(random.uniform(10.0, 1000.0), 2)
                items_for_order.append(
                    (str(order_id), str(prod_id), quantity, price_unit)
                )  # Convertendo UUIDs para string
                valor_total += quantity * price_unit

            cursor.execute(
                """
                INSERT INTO Pedido (id, id_cliente, data_pedido, status, valor_total)
                VALUES (%s, %s, %s, %s, %s)
            """,
                (
                    str(order_id),
                    str(client_id),
                    order_date,
                    status,
                    round(valor_total, 2),
                ),
            )  # Convertendo UUIDs para string

            # Inserir itens do pedido na tabela ItemPedido
            if items_for_order:
                cursor.executemany(
                    """
                    INSERT INTO ItemPedido (id_pedido, id_produto, quantidade, preco_unitario)
                    VALUES (%s, %s, %s, %s)
                """,
                    items_for_order,
                )
        conn.commit()
        print("Pedidos e Itens de Pedido inseridos.")

        # --- 4. Popular Pagamentos ---
        print(f"Populando {NUM_PAGAMENTOS} pagamentos no PostgreSQL...")
        payment_types = ["cartão", "pix", "boleto"]
        payment_status = ["aprovado", "pendente", "recusado"]
        for _ in range(NUM_PAGAMENTOS):
            payment_id = uuid.uuid4()
            order_id = random.choice(order_ids)
            payment_date = fake.date_time_between(start_date="-6m", end_date="now")
            cursor.execute(
                """
                INSERT INTO Pagamento (id, id_pedido, tipo, status, data_pagamento)
                VALUES (%s, %s, %s, %s, %s)
            """,
                (
                    str(payment_id),  # Convertendo UUID para string
                    str(order_id),  # Convertendo UUID para string
                    random.choice(payment_types),
                    random.choice(payment_status),
                    payment_date,
                ),
            )
        conn.commit()
        print("Pagamentos inseridos.")

        end_time = time.time()
        print(
            f"\nPopulação do PostgreSQL concluída em {end_time - start_time:.2f} segundos."
        )

    except OperationalError as e:
        print(f"Erro de operação ao popular PostgreSQL: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Erro inesperado ao popular PostgreSQL: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    populate_postgres()
