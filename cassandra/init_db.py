from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.io.geventreactor import (
    GeventConnection,
)
import time

KEYSPACE = "techmarket_ks"


def create_keyspace_and_tables_cassandra(session):
    try:

        session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};
        """
        )
        print(f"Keyspace '{KEYSPACE}' criado ou já existente.")
        session.set_keyspace(KEYSPACE)

        session.execute(
            """
            CREATE TABLE IF NOT EXISTS clientes_por_email (
                email text PRIMARY KEY,
                id_cliente uuid,
                nome text,
                telefone text,
                data_cadastro timestamp,
                cpf text
                -- Para os últimos 3 pedidos, idealmente buscaríamos em outra tabela
                -- ou teríamos uma lista de IDs de pedidos aqui para joins no cliente (não otimizado em Cassandra)
                -- ou uma lista de MAPs se fosse algo muito pequeno e pouco mutável
            );
        """
        )
        print("Tabela 'clientes_por_email' criada ou já existente.")

        session.execute(
            """
            CREATE TABLE IF NOT EXISTS produtos_por_categoria (
                categoria text,
                preco decimal,
                id_produto uuid,
                nome text,
                estoque int,
                PRIMARY KEY ((categoria), preco, id_produto) -- preco como clustering key para ordenação
            ) WITH CLUSTERING ORDER BY (preco ASC);
        """
        )
        print("Tabela 'produtos_por_categoria' criada ou já existente.")

        session.execute(
            """
            CREATE TABLE IF NOT EXISTS pedidos_por_cliente_status (
                id_cliente uuid,
                status text,
                data_pedido timestamp,
                id_pedido uuid,
                valor_total decimal,
                PRIMARY KEY ((id_cliente, status), data_pedido, id_pedido) -- data_pedido para ordenação
            ) WITH CLUSTERING ORDER BY (data_pedido DESC);
        """
        )
        print("Tabela 'pedidos_por_cliente_status' criada ou já existente.")

        session.execute(
            """
            CREATE TABLE IF NOT EXISTS pagamentos_por_tipo_mes (
                tipo text,
                ano_mes text, -- Ex: '2025-05' - Chave de partição composta para o tipo e mês/ano
                data_pagamento timestamp,
                id_pagamento uuid,
                id_pedido uuid,
                status text,
                PRIMARY KEY ((tipo, ano_mes), data_pagamento, id_pagamento)
            ) WITH CLUSTERING ORDER BY (data_pagamento DESC);
        """
        )
        print("Tabela 'pagamentos_por_tipo_mes' criada ou já existente.")

        session.execute(
            """
            CREATE TABLE IF NOT EXISTS pedidos_base (
                id_pedido uuid PRIMARY KEY,
                id_cliente uuid,
                data_pedido timestamp,
                status text,
                valor_total decimal
                -- Aqui podemos ter itens do pedido denormalizados como um list<map<text,text>>
                -- ou como uma tabela separada para ItemPedido se precisar ser consultado isoladamente.
            );
        """
        )
        print("Tabela 'pedidos_base' criada ou já existente.")

        session.execute(
            """
            CREATE TABLE IF NOT EXISTS pagamentos_base (
                id_pagamento uuid PRIMARY KEY,
                id_pedido uuid,
                tipo text,
                status text,
                data_pagamento timestamp
            );
        """
        )
        print("Tabela 'pagamentos_base' criada ou já existente.")

        print("Todas as tabelas do Cassandra criadas com sucesso no keyspace!")

    except NoHostAvailable as e:
        print(
            f"Erro: Nenhum host Cassandra disponível ao criar chaves e tabelas. Detalhes: {e}"
        )
    except Exception as e:
        print(f"Erro inesperado ao criar chaves e tabelas no Cassandra: {e}")


def connect_to_cassandra():
    """Conecta ao cluster Cassandra e retorna o objeto de sessão."""
    cluster = None
    session = None
    try:
        cluster = Cluster(["localhost"], port=9042, connection_class=GeventConnection, connect_timeout=200)
        session = cluster.connect()
        print("Conexão ao Cassandra estabelecida com sucesso!")
        return session
    except NoHostAvailable as e:
        print(f"Erro ao conectar ao Cassandra: {e}")
        return None
    except Exception as e:
        print(f"Ocorreu um erro inesperado ao conectar ao Cassandra: {e}")
        return None


if __name__ == "__main__":
    max_retries = 10
    retry_delay = 10

    for i in range(max_retries):
        print(f"Tentando conectar ao Cassandra... Tentativa {i + 1}/{max_retries}")
        session = connect_to_cassandra()
        if session:
            create_keyspace_and_tables_cassandra(session)
            session.shutdown()
            session.cluster.shutdown()
            print("Conexão ao Cassandra fechada.")
            break
        else:
            time.sleep(retry_delay)
    else:
        print(
            "Não foi possível conectar ao Cassandra após várias tentativas. Verifique se o Docker está rodando e se o Cassandra está totalmente inicializado."
        )
