
import sqlite3
from pathlib import Path

CAMINHO_BANCO = Path(__file__).parent / "transacoes.db"


def conectar():
    """Abre uma conexão com o banco SQLite."""
    conexao = sqlite3.connect(CAMINHO_BANCO)
    conexao.row_factory = sqlite3.Row
    return conexao


def inicializar_banco():
    """Cria a tabela de transações caso ela ainda não exista."""
    with conectar() as conexao:
        conexao.execute("""
            CREATE TABLE IF NOT EXISTS transacoes (
                id TEXT PRIMARY KEY,
                cliente_id TEXT NOT NULL,
                valor REAL NOT NULL,
                cidade TEXT NOT NULL,
                data_hora TEXT NOT NULL,
                fraude INTEGER NOT NULL DEFAULT 0,
                tipo_fraude TEXT
            )
        """)

    print(f"Banco SQLite inicializado em: {CAMINHO_BANCO}")


def salvar_transacao(transacao, tipos_fraude=None):
    """Salva a transação e os tipos de fraude identificados."""
    tipos_fraude = tipos_fraude or []

    with conectar() as conexao:
        conexao.execute("""
            INSERT INTO transacoes (
                id,
                cliente_id,
                valor,
                cidade,
                data_hora,
                fraude,
                tipo_fraude
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                cliente_id = excluded.cliente_id,
                valor = excluded.valor,
                cidade = excluded.cidade,
                data_hora = excluded.data_hora,
                fraude = excluded.fraude,
                tipo_fraude = excluded.tipo_fraude
        """, (
            transacao["id"],
            transacao["cliente_id"],
            transacao["valor"],
            transacao["cidade"],
            transacao["data_hora"],
            1 if tipos_fraude else 0,
            ", ".join(tipos_fraude) if tipos_fraude else None
        ))


def listar_transacoes():
    """Retorna todas as transações armazenadas."""
    with conectar() as conexao:
        return conexao.execute("""
            SELECT *
            FROM transacoes
            ORDER BY data_hora
        """).fetchall()


if __name__ == "__main__":
    inicializar_banco()
