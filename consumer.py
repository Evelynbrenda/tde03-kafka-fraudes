
import json
from collections import defaultdict, deque
from datetime import datetime, timedelta

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from database import inicializar_banco, salvar_transacao

KAFKA_SERVERS = "localhost:9093"
TOPIC = "transacoes"
CONSUMER_GROUP = "detector-fraudes"


def converter_data(data_texto):
    """Converte a data ISO recebida para um datetime com fuso horário."""
    data = datetime.fromisoformat(data_texto)
    if data.tzinfo is None:
        raise ValueError("A data da transação precisa conter fuso horário.")
    return data


class DetectorFraudes:
    def __init__(self):
        self.transacoes_por_cliente = defaultdict(deque)
        self.ultima_transacao_por_cliente = defaultdict(deque)

    def detectar(self, transacao):
        """Aplica as regras de fraude e retorna os códigos identificados."""
        tipos_fraude = []

        cliente_id = transacao["cliente_id"]
        valor = float(transacao["valor"])
        cidade = transacao["cidade"]
        data_atual = converter_data(transacao["data_hora"])

        # Regra 1: transação de alto valor.
        if valor >= 10000:
            tipos_fraude.append("ALTO_VALOR")

        # Regra 2: quatro transações do mesmo cliente em menos de 60 segundos.
        historico = self.transacoes_por_cliente[cliente_id]
        limite_60s = data_atual - timedelta(seconds=60)

        while historico and historico[0] <= limite_60s:
            historico.popleft()

        historico.append(data_atual)

        if (
            len(historico) >= 4
            and (data_atual - historico[0]).total_seconds() < 60
        ):
            tipos_fraude.append("TEMPO_60s")

        # Regra 3: duas transações em cidades diferentes dentro de 10 minutos.
        historico_geo = self.ultima_transacao_por_cliente[cliente_id]
        limite_10m = data_atual - timedelta(minutes=10)

        while historico_geo and historico_geo[0][0] <= limite_10m:
         historico_geo.popleft()

        if any(cidade_anterior != cidade for _, cidade_anterior in historico_geo):
            tipos_fraude.append("GEO_10m")

        historico_geo.append((data_atual, cidade))

        return tipos_fraude


def criar_consumidor():
    """Conecta ao Kafka e decodifica o JSON de cada mensagem."""
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda mensagem: json.loads(
            mensagem.decode("utf-8")
        )
    )


def main():
    inicializar_banco()
    detector = DetectorFraudes()
    consumidor = None

    try:
        consumidor = criar_consumidor()
        print(f"Consumidor conectado ao Kafka em {KAFKA_SERVERS}")
        print(f"Lendo o tópico '{TOPIC}'. Pressione Ctrl+C para encerrar.\n")

        for mensagem in consumidor:
            transacao = mensagem.value

            try:
                tipos_fraude = detector.detectar(transacao)
                salvar_transacao(transacao, tipos_fraude)

                if tipos_fraude:
                    print("\n🚨 ALERTA DE FRAUDE")
                    print(f"Cliente: {transacao['cliente_id']}")
                    print(f"Valor: R$ {float(transacao['valor']):.2f}")
                    print(f"Cidade: {transacao['cidade']}")
                    print(f"Tipos: {', '.join(tipos_fraude)}")
                    print(f"ID: {transacao['id']}\n")
                else:
                    print(
                        f"[OK] Cliente: {transacao['cliente_id']} | "
                        f"Valor: R$ {float(transacao['valor']):.2f} | "
                        f"Cidade: {transacao['cidade']}"
                    )

                consumidor.commit()

            except (KeyError, TypeError, ValueError) as erro:
                print(f"[ERRO] Mensagem inválida: {erro}")

    except KeyboardInterrupt:
        print("\nConsumidor encerrado pelo usuário.")

    except KafkaError as erro:
        print(f"[ERRO] Falha no Kafka: {erro}")

    finally:
        if consumidor is not None:
            consumidor.close()
            print("Conexão com o Kafka encerrada.")


if __name__ == "__main__":
    main()
