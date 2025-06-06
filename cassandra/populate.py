from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.io.geventreactor import GeventConnection
from faker import Faker
import random
import time
from datetime import datetime, timedelta
import uuid

fake = Faker("pt_BR")

KEYSPACE = "techmarket_ks"
NUM_CLIENTES = 20000
NUM_PRODUTOS = 5000
NUM_PEDIDOS = 30000




def connect_to_cassandra():
    cluster = None
    session = None
    try:
        cluster = Cluster(
            ["localhost"],
            port=9042,
            connection_class=GeventConnection,
            connect_timeout=30,
        )  
        session = cluster.connect()
        session.set_keyspace(KEYSPACE)
        return session
    except NoHostAvailable as e:
        print(f"Erro ao conectar ao Cassandra: {e}")
        return None
    except Exception as e:
        print(f"Ocorreu um erro inesperado ao conectar ao Cassandra: {e}")
        return None


def populate_cassandra():
    session = connect_to_cassandra()
    if not session:
        print("Não foi possível conectar ao Cassandra. Encerrando população.")
        return

    start_time = time.time()

    try:
        
        print(f"Populando {NUM_CLIENTES} clientes no Cassandra (clientes_por_email)...")
        client_ids = []
        for _ in range(NUM_CLIENTES):
            client_id = uuid.uuid4()
            client_ids.append(client_id)
            email = fake.unique.email()
            session.execute(
                """
                INSERT INTO clientes_por_email (email, id_cliente, nome, telefone, data_cadastro, cpf)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    email,
                    client_id,
                    fake.name(),
                    fake.phone_number(),
                    fake.date_time_between(start_date="-2y", end_date="now"),
                    fake.unique.cpf(),
                ),
            )
        print("Clientes inseridos.")

        
        print(
            f"Populando {NUM_PRODUTOS} produtos no Cassandra (produtos_por_categoria)..."
        )
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
            categoria = random.choice(categories)
            preco = round(random.uniform(10.0, 5000.0), 2)
            session.execute(
                """
                INSERT INTO produtos_por_categoria (categoria, preco, id_produto, nome, estoque)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    categoria,
                    preco,
                    product_id,
                    fake.word().capitalize() + " " + fake.word() + " " + fake.word(),
                    random.randint(0, 1000),
                ),
            )
        print("Produtos inseridos.")

        
        print(
            f"Populando {NUM_PEDIDOS} pedidos e {NUM_PEDIDOS} pagamentos no Cassandra..."
        )
        order_ids = []
        payment_types = ["cartão", "pix", "boleto"]
        payment_status_options = ["aprovado", "pendente", "recusado"]
        order_status_options = ["pendente", "processando", "entregue", "cancelado"]

        for _ in range(NUM_PEDIDOS):
            order_id = uuid.uuid4()
            order_ids.append(order_id)
            client_id = random.choice(client_ids)
            order_date = fake.date_time_between(start_date="-1y", end_date="now")
            order_status = random.choice(order_status_options)

            num_items = random.randint(1, 5)
            valor_total = 0

            selected_product_ids = random.sample(
                product_ids, min(num_items, len(product_ids))
            )

            
            valor_total = round(random.uniform(50.0, 5000.0), 2)

            
            session.execute(
                """
                INSERT INTO pedidos_base (id_pedido, id_cliente, data_pedido, status, valor_total)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (order_id, client_id, order_date, order_status, valor_total),
            )

            
            session.execute(
                """
                INSERT INTO pedidos_por_cliente_status (id_cliente, status, data_pedido, id_pedido, valor_total)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (client_id, order_status, order_date, order_id, valor_total),
            )

            
            payment_id = uuid.uuid4()
            payment_type = random.choice(payment_types)
            payment_status = random.choice(payment_status_options)
            payment_date = fake.date_time_between(start_date="-6m", end_date="now")
            ano_mes = payment_date.strftime("%Y-%m")

            
            session.execute(
                """
                INSERT INTO pagamentos_base (id_pagamento, id_pedido, tipo, status, data_pagamento)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (payment_id, order_id, payment_type, payment_status, payment_date),
            )

            
            session.execute(
                """
                INSERT INTO pagamentos_por_tipo_mes (tipo, ano_mes, data_pagamento, id_pagamento, id_pedido, status)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    payment_type,
                    ano_mes,
                    payment_date,
                    payment_id,
                    order_id,
                    payment_status,
                ),
            )
        print("Pedidos e Pagamentos inseridos.")

        end_time = time.time()
        print(
            f"\nPopulação do Cassandra concluída em {end_time - start_time:.2f} segundos."
        )

    except NoHostAvailable as e:
        print(f"Erro ao popular Cassandra: Nenhum host disponível. Detalhes: {e}")
    except Exception as e:
        print(f"Erro inesperado ao popular Cassandra: {e}")
    finally:
        session.shutdown()
        session.cluster.shutdown()


if __name__ == "__main__":
    populate_cassandra()
