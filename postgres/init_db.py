import psycopg2
from psycopg2 import OperationalError
import time
import uuid


def create_tables_postgres(conn):
    cursor = conn.cursor()
    try:

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Cliente (
                id UUID PRIMARY KEY,
                nome VARCHAR(255) NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                telefone VARCHAR(20),
                data_cadastro TIMESTAMP NOT NULL,
                cpf VARCHAR(14) UNIQUE NOT NULL
            );
        """
        )
        print("Tabela 'Cliente' criada ou já existente.")

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_cliente_email ON Cliente (email);"
        )
        print("Índice 'idx_cliente_email' criado ou já existente.")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Produto (
                id UUID PRIMARY KEY,
                nome VARCHAR(255) NOT NULL,
                categoria VARCHAR(100) NOT NULL,
                preco DECIMAL(10, 2) NOT NULL,
                estoque INT NOT NULL
            );
        """
        )
        print("Tabela 'Produto' criada ou já existente.")

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_produto_categoria ON Produto (categoria);"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_produto_preco ON Produto (preco);"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_produto_categoria_preco ON Produto (categoria, preco);"
        )
        print("Índices de 'Produto' criados ou já existentes.")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Pedido (
                id UUID PRIMARY KEY,
                id_cliente UUID NOT NULL,
                data_pedido TIMESTAMP NOT NULL,
                status VARCHAR(50) NOT NULL,
                valor_total DECIMAL(10, 2) NOT NULL,
                FOREIGN KEY (id_cliente) REFERENCES Cliente(id)
            );
        """
        )
        print("Tabela 'Pedido' criada ou já existente.")

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_pedido_id_cliente ON Pedido (id_cliente);"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_pedido_status ON Pedido (status);"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_pedido_data_pedido ON Pedido (data_pedido);"
        )
        print("Índices de 'Pedido' criados ou já existentes.")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ItemPedido (
                id_pedido UUID NOT NULL,
                id_produto UUID NOT NULL,
                quantidade INT NOT NULL,
                preco_unitario DECIMAL(10, 2) NOT NULL,
                PRIMARY KEY (id_pedido, id_produto),
                FOREIGN KEY (id_pedido) REFERENCES Pedido(id),
                FOREIGN KEY (id_produto) REFERENCES Produto(id)
            );
        """
        )
        print("Tabela 'ItemPedido' criada ou já existente.")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS Pagamento (
                id UUID PRIMARY KEY,
                id_pedido UUID NOT NULL,
                tipo VARCHAR(50) NOT NULL,
                status VARCHAR(50) NOT NULL,
                data_pagamento TIMESTAMP NOT NULL,
                FOREIGN KEY (id_pedido) REFERENCES Pedido(id)
            );
        """
        )
        print("Tabela 'Pagamento' criada ou já existente.")

        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_pagamento_tipo ON Pagamento (tipo);"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_pagamento_data_pagamento ON Pagamento (data_pagamento);"
        )
        print("Índices de 'Pagamento' criados ou já existentes.")

        conn.commit()
        print("Todas as tabelas e índices do PostgreSQL criados com sucesso!")

    except OperationalError as e:
        print(f"Erro de operação ao criar tabelas no PostgreSQL: {e}")
        conn.rollback()
    except Exception as e:
        print(f"Erro inesperado ao criar tabelas no PostgreSQL: {e}")
        conn.rollback()
    finally:
        cursor.close()


def connect_to_postgres():
    conn = None
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="mysecretpassword",
            port="5432",
        )
        print("Conexão ao PostgreSQL estabelecida com sucesso!")
        return conn
    except OperationalError as e:
        print(f"Erro ao conectar ao PostgreSQL: {e}")
        return None


if __name__ == "__main__":
    max_retries = 10
    retry_delay = 5

    for i in range(max_retries):
        print(f"Tentando conectar ao PostgreSQL... Tentativa {i + 1}/{max_retries}")
        conn = connect_to_postgres()
        if conn:
            create_tables_postgres(conn)
            conn.close()
            print("Conexão ao PostgreSQL fechada.")
            break
        else:
            time.sleep(retry_delay)
    else:
        print(
            "Não foi possível conectar ao PostgreSQL após várias tentativas. Verifique se o Docker está rodando."
        )
