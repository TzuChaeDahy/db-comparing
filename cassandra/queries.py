from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.io.geventreactor import GeventConnection
import time
from datetime import datetime, timedelta
import uuid

# --- Configurações de Conexão ---
CASSANDRA_HOSTS = ["localhost"]
CASSANDRA_PORT = 9042
KEYSPACE = "techmarket_ks"
NUM_RUNS = 10  # Número de vezes que cada consulta será executada para calcular a média


def connect_to_cassandra():
    """Conecta ao cluster Cassandra e retorna o objeto de sessão."""
    cluster = None
    session = None
    try:
        cluster = Cluster(
            CASSANDRA_HOSTS,
            port=CASSANDRA_PORT,
            connection_class=GeventConnection,
            connect_timeout=30,
        )  # Aumentado o timeout
        session = cluster.connect()
        session.set_keyspace(KEYSPACE)
        print("Conexão ao Cassandra estabelecida.")
        return session
    except NoHostAvailable as e:
        print(f"Erro ao conectar ao Cassandra: {e}")
        return None
    except Exception as e:
        print(f"Ocorreu um erro inesperado ao conectar ao Cassandra: {e}")
        return None


def execute_cql_query(session, query_cql, params=None):
    """Executa uma consulta CQL e mede o tempo."""
    start_time = time.time()
    if params:
        rows = session.execute(query_cql, params)
    else:
        rows = session.execute(query_cql)
    results = list(rows)  # Convert all rows to a list to ensure full processing
    end_time = time.time()
    return (end_time - start_time) * 1000, results  # Tempo em milissegundos


def run_cassandra_queries():
    session = connect_to_cassandra()
    if not session:
        print("Não foi possível conectar ao Cassandra. Encerrando consultas.")
        return

    try:
        # --- Q1: Buscar cliente por email e listar seus últimos 3 pedidos ---
        print(
            "\n--- Executando Q1: Buscar cliente por email e listar seus últimos 3 pedidos ---"
        )
        # Para esta consulta, como a modelagem do Cassandra é denormalizada,
        # buscaríamos o cliente pela tabela 'clientes_por_email' e,
        # se os pedidos não estivessem aninhados, teríamos que consultar outra tabela de pedidos.
        # Aqui, vamos pegar um email de cliente e buscar diretamente.
        # Nota: Cassandra não faz JOINs. O "últimos 3 pedidos" precisaria ser pré-calculado
        # e armazenado na tabela do cliente ou buscado em uma tabela de pedidos por cliente com ordenação.

        # Obter um email de cliente de exemplo
        rows = session.execute(
            "SELECT email, id_cliente FROM clientes_por_email LIMIT 1;"
        )
        sample_client = rows.one()  # Pega a primeira linha
        if sample_client:
            client_email = sample_client.email
            client_id = sample_client.id_cliente

            q1_times = []
            for _ in range(NUM_RUNS):
                # Consulta na tabela de clientes (para pegar infos do cliente)
                query_cliente = """
                    SELECT nome, email FROM clientes_por_email WHERE email = %s;
                """
                # Consulta na tabela de pedidos por cliente e status (para pegar pedidos recentes, mesmo que não aninhados)
                # Assumimos que pedidos_por_cliente_status pode nos dar os pedidos ordenados por data.
                query_pedidos = """
                    SELECT id_pedido, data_pedido, status, valor_total
                    FROM pedidos_por_cliente_status
                    WHERE id_cliente = %s
                    LIMIT 3; -- Limitar a 3 pedidos
                """
                # No Cassandra, isso seriam duas consultas separadas que seriam combinadas na aplicação.
                # Medimos o tempo das duas para ter uma ideia do custo.
                time_taken_cliente, client_info = execute_cql_query(
                    session, query_cliente, (client_email,)
                )
                time_taken_pedidos, recent_orders = execute_cql_query(
                    session, query_pedidos, (client_id,)
                )
                q1_times.append(
                    time_taken_cliente + time_taken_pedidos
                )  # Soma os tempos

            avg_time = sum(q1_times) / NUM_RUNS
            print(f"Média de tempo (Q1): {avg_time:.2f} ms")
            print(
                f"Exemplo de resultado (Q1 - cliente {client_email}): Cliente: {client_info[0] if client_info else 'N/A'}, Pedidos: {recent_orders[0] if recent_orders else 'Nenhum pedido.'}"
            )
        else:
            print("Nenhum cliente encontrado para testar Q1.")

        # --- Q2: Listar produtos de uma categoria ordenados por preço ---
        print(
            "\n--- Executando Q2: Listar produtos de uma categoria ordenados por preço ---"
        )
        # Obter uma categoria de produto de exemplo
        rows = session.execute("SELECT categoria FROM produtos_por_categoria LIMIT 1;")
        sample_product = rows.one()
        if sample_product:
            product_category = sample_product.categoria
            q2_times = []
            for _ in range(NUM_RUNS):
                query_cql = """
                    SELECT nome, categoria, preco, estoque
                    FROM produtos_por_categoria
                    WHERE categoria = %s
                    LIMIT 100; -- Limitar para não puxar todos os produtos se a categoria for grande
                """
                time_taken, results = execute_cql_query(
                    session, query_cql, (product_category,)
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
        # Obter um ID de cliente de exemplo
        rows = session.execute("SELECT id_cliente FROM clientes_por_email LIMIT 1;")
        sample_client_q3 = rows.one()
        if sample_client_q3:
            client_id_q3 = sample_client_q3.id_cliente
            q3_times = []
            for _ in range(NUM_RUNS):
                query_cql = """
                    SELECT id_pedido, data_pedido, valor_total
                    FROM pedidos_por_cliente_status
                    WHERE id_cliente = %s AND status = 'entregue'
                    LIMIT 100; -- Limitar para não puxar todos os pedidos
                """
                time_taken, results = execute_cql_query(
                    session, query_cql, (client_id_q3,)
                )
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
        # OBSERVAÇÃO: Esta consulta é de agregação complexa e não é ideal para o Cassandra em tempo real.
        # Uma solução típica no Cassandra seria:
        # 1. Processar dados offline (e.g., com Apache Spark) para calcular os top 5.
        # 2. Armazenar os resultados em uma tabela Cassandra separada (ex: 'produtos_top_vendidos').
        # A consulta abaixo é uma simulação e não o ideal para grandes volumes no Cassandra.
        # Se tivéssemos uma tabela 'produtos_top_vendidos' atualizada periodicamente:
        # SELECT produto_id, nome_produto, total_vendido FROM produtos_top_vendidos LIMIT 5;

        # SIMULAÇÃO: Selecionar 5 produtos aleatórios (não "mais vendidos" de fato, pois requer agregação)
        # OU: Se houvesse uma coluna pré-agregada na tabela de produtos base, seria por lá.
        # Para este exercício, vamos apenas selecionar 5 produtos da tabela de produtos_por_categoria
        # ou, idealmente, de uma tabela que já contivesse o rank.
        q4_times = []
        for _ in range(NUM_RUNS):
            query_cql = """
                SELECT nome, categoria, preco
                FROM produtos_por_categoria
                LIMIT 5; -- Apenas um exemplo, não os "mais vendidos" agregados.
            """
            time_taken, results = execute_cql_query(session, query_cql)
            q4_times.append(time_taken)
        avg_time = sum(q4_times) / NUM_RUNS
        print(f"Média de tempo (Q4 - Simulado): {avg_time:.2f} ms")
        print(
            f"Exemplo de resultado (Q4 - Simulado): {results[0] if results else 'Nenhum produto encontrado.'}"
        )

        # --- Q5: Consultar pagamentos feitos via PIX no último mês ---
        print(
            "\n--- Executando Q5: Consultar pagamentos feitos via PIX no último mês ---"
        )
        end_date_q5 = datetime.now()
        start_date_q5 = end_date_q5 - timedelta(days=30)
        # Para Cassandra, ano_mes é uma chave de partição. Precisamos especificar o mês/ano exato.
        # Se a consulta é "último mês", teríamos que saber qual mês é o "último mês" para a chave de partição.
        # Para simplificar, vamos usar o mês/ano atual como exemplo.
        current_year_month = datetime.now().strftime("%Y-%m")

        q5_times = []
        for _ in range(NUM_RUNS):
            query_cql = """
                SELECT id_pagamento, id_pedido, status, data_pagamento
                FROM pagamentos_por_tipo_mes
                WHERE tipo = 'pix' AND ano_mes = %s AND data_pagamento BETWEEN %s AND %s
                LIMIT 100;
            """
            time_taken, results = execute_cql_query(
                session, query_cql, (current_year_month, start_date_q5, end_date_q5)
            )
            q5_times.append(time_taken)
        avg_time = sum(q5_times) / NUM_RUNS
        print(f"Média de tempo (Q5): {avg_time:.2f} ms")
        print(
            f"Exemplo de resultado (Q5 - PIX no mês {current_year_month}): {results[0] if results else 'Nenhum pagamento PIX encontrado.'}"
        )

        # --- Q6: Obter o valor total gasto por um cliente em pedidos realizados em um determinado período (ex.: último 3 meses) ---
        print(
            "\n--- Executando Q6: Obter o valor total gasto por um cliente em pedidos em um período ---"
        )
        # OBSERVAÇÃO: Similar a Q4, essa é uma agregação que não é a força do Cassandra em tempo real.
        # O ideal seria ter uma tabela materializada com totais já calculados ou usar ferramentas analíticas.

        # Obter um ID de cliente de exemplo
        rows = session.execute("SELECT id_cliente FROM clientes_por_email LIMIT 1;")
        sample_client_q6 = rows.one()
        if sample_client_q6:
            client_id_q6 = sample_client_q6.id_cliente
            end_date_q6 = datetime.now()
            start_date_q6 = end_date_q6 - timedelta(
                days=90
            )  # Aproximadamente 3 meses atrás

            q6_times = []
            for _ in range(NUM_RUNS):
                # Para simular, vamos buscar todos os pedidos do cliente no período e somar no Python.
                # Isso NÃO É performático para grandes volumes, mas demonstra o acesso aos dados.
                # A tabela pedidos_por_cliente_status é otimizada para filtrar por cliente e status,
                # mas não diretamente para soma de valor em um período.
                query_cql = """
                    SELECT valor_total
                    FROM pedidos_por_cliente_status
                    WHERE id_cliente = %s AND data_pedido BETWEEN %s AND %s
                    LIMIT 10000; -- Limitar para evitar trazer dados demais na simulação
                """
                time_taken, results = execute_cql_query(
                    session, query_cql, (client_id_q6, start_date_q6, end_date_q6)
                )
                total_gasto = sum(row.valor_total for row in results) if results else 0
                q6_times.append(
                    time_taken
                )  # Medimos o tempo de busca, não o de soma no Python
            avg_time = sum(q6_times) / NUM_RUNS
            print(f"Média de tempo (Q6 - Simulado): {avg_time:.2f} ms")
            print(
                f"Exemplo de resultado (Q6 - cliente {client_id_q6}): Total gasto (estimado): {total_gasto:.2f}"
            )
        else:
            print("Nenhum cliente encontrado para testar Q6.")

    except NoHostAvailable as e:
        print(
            f"Erro ao executar consultas no Cassandra: Nenhum host disponível. Detalhes: {e}"
        )
    except Exception as e:
        print(f"Erro inesperado ao executar consultas no Cassandra: {e}")
    finally:
        session.shutdown()
        session.cluster.shutdown()
        print("\nConexão ao Cassandra fechada.")


if __name__ == "__main__":
    run_cassandra_queries()
