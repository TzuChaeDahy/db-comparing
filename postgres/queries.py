import psycopg2
from psycopg2 import OperationalError
import time
from datetime import datetime, timedelta
import uuid
import random  # Importar para usar random.choice

# --- Configurações de Conexão ---
DB_HOST = "localhost"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "mysecretpassword"
DB_PORT = "5432"

NUM_RUNS = 10  # Número de vezes que cada consulta será executada para calcular a média


def connect_to_postgres():
    """Conecta ao banco de dados PostgreSQL e retorna o objeto de conexão."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
        )
        print("Conexão ao PostgreSQL estabelecida.")
        return conn
    except OperationalError as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None


def execute_query(cursor, query_sql, params=None):
    """Executa uma consulta SQL e mede o tempo."""
    start_time = time.time()
    if params:
        cursor.execute(query_sql, params)
    else:
        cursor.execute(query_sql)
    # Fetch all results to ensure the query is fully processed
    results = cursor.fetchall()
    end_time = time.time()
    return (end_time - start_time) * 1000, results  # Tempo em milissegundos


def run_postgres_queries():
    conn = connect_to_postgres()
    if not conn:
        print("Não foi possível conectar ao PostgreSQL. Encerrando consultas.")
        return

    cursor = conn.cursor()

    try:
        # --- Q1: Buscar cliente por email e listar seus últimos 3 pedidos ---
        print(
            "\n--- Executando Q1: Buscar cliente por email e listar seus últimos 3 pedidos ---"
        )
        # Obter um email de cliente existente para a consulta
        # Buscamos um número limitado de clientes e escolhemos um aleatoriamente para dinamizar
        cursor.execute(
            "SELECT email, id FROM Cliente OFFSET floor(random() * (SELECT COUNT(*) FROM Cliente)) LIMIT 1;"
        )
        sample_client = cursor.fetchone()
        if sample_client:
            client_email, client_id = sample_client
            q1_times = []
            for _ in range(NUM_RUNS):
                # Usando um JOIN para obter cliente e seus pedidos
                query_sql = """
                    SELECT
                        c.nome, c.email, p.id, p.data_pedido, p.status, p.valor_total
                    FROM
                        Cliente c
                    JOIN
                        Pedido p ON c.id = p.id_cliente
                    WHERE
                        c.email = %s
                    ORDER BY
                        p.data_pedido DESC
                    LIMIT 3;
                """
                time_taken, results = execute_query(cursor, query_sql, (client_email,))
                q1_times.append(time_taken)
            avg_time = sum(q1_times) / NUM_RUNS
            print(f"Média de tempo (Q1): {avg_time:.2f} ms")
            print(
                f"Exemplo de resultado (Q1 - cliente {client_email}): {results[0] if results else 'Nenhum pedido encontrado.'}"
            )
        else:
            print("Nenhum cliente encontrado para testar Q1.")

        # --- Q2: Listar produtos de uma categoria ordenados por preço ---
        print(
            "\n--- Executando Q2: Listar produtos de uma categoria ordenados por preço ---"
        )
        # Obter uma categoria existente aleatoriamente
        cursor.execute(
            "SELECT categoria FROM Produto GROUP BY categoria OFFSET floor(random() * (SELECT COUNT(DISTINCT categoria) FROM Produto)) LIMIT 1;"
        )
        sample_category = cursor.fetchone()
        if sample_category:
            product_category = sample_category[0]
            q2_times = []
            for _ in range(NUM_RUNS):
                query_sql = """
                    SELECT
                        nome, categoria, preco, estoque
                    FROM
                        Produto
                    WHERE
                        categoria = %s
                    ORDER BY
                        preco ASC;
                """
                time_taken, results = execute_query(
                    cursor, query_sql, (product_category,)
                )
                q2_times.append(time_taken)
            avg_time = sum(q2_times) / NUM_RUNS
            print(f"Média de tempo (Q2): {avg_time:.2f} ms")
            print(
                f"Exemplo de resultado (Q2 - categoria {product_category}): {results[0] if results else 'Nenhum produto encontrado.'}"
            )
        else:
            print("Nenhuma categoria encontrada para testar Q2.")

        # --- Q3: Listar pedidos de um cliente com status "entregue" ---
        print(
            "\n--- Executando Q3: Listar pedidos de um cliente com status 'entregue' ---"
        )
        # Obter um ID de cliente existente aleatoriamente
        cursor.execute(
            "SELECT id FROM Cliente OFFSET floor(random() * (SELECT COUNT(*) FROM Cliente)) LIMIT 1;"
        )
        sample_client_id_q3 = cursor.fetchone()
        if sample_client_id_q3:
            client_id_q3 = sample_client_id_q3[0]
            q3_times = []
            for _ in range(NUM_RUNS):
                query_sql = """
                    SELECT
                        id, data_pedido, status, valor_total
                    FROM
                        Pedido
                    WHERE
                        id_cliente = %s AND status = 'entregue'
                    ORDER BY
                        data_pedido DESC;
                """
                time_taken, results = execute_query(cursor, query_sql, (client_id_q3,))
                q3_times.append(time_taken)
            avg_time = sum(q3_times) / NUM_RUNS
            print(f"Média de tempo (Q3): {avg_time:.2f} ms")
            print(
                f"Exemplo de resultado (Q3 - cliente {client_id_q3}): {results[0] if results else 'Nenhum pedido entregue encontrado.'}"
            )
        else:
            print("Nenhum cliente encontrado para testar Q3.")

        # --- Q4: Obter os 5 produtos mais vendidos ---
        print("\n--- Executando Q4: Obter os 5 produtos mais vendidos ---")
        q4_times = []
        for _ in range(NUM_RUNS):
            query_sql = """
                SELECT
                    p.nome, p.categoria, SUM(ip.quantidade) AS total_vendido
                FROM
                    Produto p
                JOIN
                    ItemPedido ip ON p.id = ip.id_produto
                GROUP BY
                    p.id, p.nome, p.categoria
                ORDER BY
                    total_vendido DESC
                LIMIT 5;
            """
            time_taken, results = execute_query(cursor, query_sql)
            q4_times.append(time_taken)
        avg_time = sum(q4_times) / NUM_RUNS
        print(f"Média de tempo (Q4): {avg_time:.2f} ms")
        print(
            f"Exemplo de resultado (Q4): {results[0] if results else 'Nenhum produto vendido encontrado.'}"
        )

        # --- Q5: Consultar pagamentos feitos via PIX no último mês ---
        print(
            "\n--- Executando Q5: Consultar pagamentos feitos via PIX no último mês ---"
        )
        # Obter uma data de pagamento de exemplo para definir o "último mês"
        cursor.execute(
            "SELECT data_pagamento FROM Pagamento WHERE tipo = 'pix' OFFSET floor(random() * (SELECT COUNT(*) FROM Pagamento WHERE tipo = 'pix')) LIMIT 1;"
        )
        sample_payment_date = cursor.fetchone()
        if sample_payment_date:
            reference_date = sample_payment_date[0]
            # Definir o início e fim do mês da data de referência
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
                query_sql = """
                    SELECT
                        id, id_pedido, tipo, status, data_pagamento
                    FROM
                        Pagamento
                    WHERE
                        tipo = 'pix' AND data_pagamento BETWEEN %s AND %s
                    ORDER BY
                        data_pagamento DESC;
                """
                time_taken, results = execute_query(
                    cursor, query_sql, (start_date_q5, end_date_q5)
                )
                q5_times.append(time_taken)
            avg_time = sum(q5_times) / NUM_RUNS
            print(f"Média de tempo (Q5): {avg_time:.2f} ms")
            print(
                f"Exemplo de resultado (Q5 - PIX no período {start_date_q5.strftime('%Y-%m')}): {results[0] if results else 'Nenhum pagamento PIX encontrado neste período.'}"
            )
        else:
            print("Nenhum pagamento PIX encontrado para testar Q5.")

        # --- Q6: Obter o valor total gasto por um cliente em pedidos realizados em um determinado período (ex.: último 3 meses) ---
        print(
            "\n--- Executando Q6: Obter o valor total gasto por um cliente em pedidos em um período ---"
        )
        # Obter um ID de cliente existente aleatoriamente
        cursor.execute(
            "SELECT id FROM Cliente OFFSET floor(random() * (SELECT COUNT(*) FROM Cliente)) LIMIT 1;"
        )
        sample_client_id_q6 = cursor.fetchone()
        if sample_client_id_q6:
            client_id_q6 = sample_client_id_q6[0]
            # Obter uma data de pedido de exemplo para definir o "período" de 3 meses
            cursor.execute(
                "SELECT data_pedido FROM Pedido WHERE id_cliente = %s OFFSET floor(random() * (SELECT COUNT(*) FROM Pedido WHERE id_cliente = %s)) LIMIT 1;",
                (client_id_q6, client_id_q6),
            )
            sample_order_date = cursor.fetchone()

            if sample_order_date:
                reference_date_q6 = sample_order_date[0]
                end_date_q6 = reference_date_q6
                start_date_q6 = end_date_q6 - timedelta(
                    days=90
                )  # Aproximadamente 3 meses atrás

                q6_times = []
                for _ in range(NUM_RUNS):
                    query_sql = """
                        SELECT
                            SUM(valor_total) AS total_gasto
                        FROM
                            Pedido
                        WHERE
                            id_cliente = %s AND data_pedido BETWEEN %s AND %s;
                    """
                    time_taken, results = execute_query(
                        cursor, query_sql, (client_id_q6, start_date_q6, end_date_q6)
                    )
                    q6_times.append(time_taken)
                avg_time = sum(q6_times) / NUM_RUNS
                print(f"Média de tempo (Q6): {avg_time:.2f} ms")
                print(
                    f"Exemplo de resultado (Q6 - cliente {client_id_q6} no período {start_date_q6.strftime('%Y-%m-%d')} a {end_date_q6.strftime('%Y-%m-%d')}): {results[0] if results else 'Nenhum gasto encontrado para o cliente no período.'}"
                )
            else:
                print(
                    f"Nenhum pedido encontrado para o cliente {client_id_q6} para definir o período em Q6."
                )
        else:
            print("Nenhum cliente encontrado para testar Q6.")

    except OperationalError as e:
        print(f"Erro de operação ao executar consultas no PostgreSQL: {e}")
    except Exception as e:
        print(f"Erro inesperado ao executar consultas no PostgreSQL: {e}")
    finally:
        cursor.close()
        conn.close()
        print("\nConexão ao PostgreSQL fechada.")


if __name__ == "__main__":
    run_postgres_queries()
