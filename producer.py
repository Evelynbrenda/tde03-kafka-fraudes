
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_SERVERS = "localhost:9093"
TOPIC = "transacoes"
INTERVALO_SEGUNDOS = 3

CLIENTES = ["CLI001", "CLI002", "CLI003", "CLI004", "CLI005"]
CIDADES = ["Crato", "Juazeiro do Norte", "Barbalha", "Fortaleza", "Recife"]


def gerar_transacao():
    """Gera uma transação fictícia para simular compras."""
    return {
        "id": str(uuid.uuid4()),
        "cliente_id": random.choice(CLIENTES),
        "valor": round(random.uniform(50, 15000), 2),
        "cidade": random.choice(CIDADES),
        "data_hora": datetime.now(timezone.utc).isoformat()
    }


def criar_produtor():
    """Conecta o produtor Python ao Kafka do Docker."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda dados: json.dumps(dados).encode("utf-8"),
        acks="all",
        retries=5
    )


def main():
    produtor = None

    try:
        produtor = criar_produtor()
        print(f"Produtor conectado ao Kafka em {KAFKA_SERVERS}")
        print(f"Enviando transações para o tópico '{TOPIC}' a cada {INTERVALO_SEGUNDOS} segundos.")
        print("Pressione Ctrl+C para encerrar.\n")

        while True:
            transacao = gerar_transacao()

            try:
                resultado = produtor.send(TOPIC, value=transacao)
                resultado.get(timeout=10)

                print(
                    f"[ENVIADA] Cliente: {transacao['cliente_id']} | "
                    f"Valor: R$ {transacao['valor']:.2f} | "
                    f"Cidade: {transacao['cidade']} | "
                    f"ID: {transacao['id']}"
                )

            except KafkaError as erro:
                print(f"[ERRO] Não foi possível enviar a transação: {erro}")

            time.sleep(INTERVALO_SEGUNDOS)

    except KeyboardInterrupt:
        print("\nProdutor encerrado pelo usuário.")

    except Exception as erro:
        print(f"[ERRO] Falha ao conectar ou executar o produtor: {erro}")

    finally:
        if produtor is not None:
            produtor.flush()
            produtor.close()
            print("Conexão com o Kafka encerrada.")


if __name__ == "__main__":
    main()
