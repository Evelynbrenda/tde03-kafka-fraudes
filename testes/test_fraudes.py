
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from consumer import DetectorFraudes


class TestDetectorFraudes(unittest.TestCase):
    def setUp(self):
        self.detector = DetectorFraudes()
        self.inicio = datetime(
            2026, 10, 16, 12, 0, 0, tzinfo=timezone.utc
        )

    def criar_transacao(
        self, segundos=0, cliente="CLI001",
        valor=100.00, cidade="Crato"
    ):
        data_hora = self.inicio + timedelta(seconds=segundos)

        return {
            "id": f"{cliente}-{segundos}-{valor}-{cidade}",
            "cliente_id": cliente,
            "valor": valor,
            "cidade": cidade,
            "data_hora": data_hora.isoformat(),
        }

    def test_alto_valor_no_limite(self):
        resultado = self.detector.detectar(
            self.criar_transacao(valor=10000)
        )
        self.assertIn("ALTO_VALOR", resultado)

    def test_valor_abaixo_do_limite(self):
        resultado = self.detector.detectar(
            self.criar_transacao(valor=9999.99)
        )
        self.assertNotIn("ALTO_VALOR", resultado)

    def test_quatro_transacoes_em_menos_de_60_segundos(self):
        resultado = []

        for segundo in (0, 10, 20, 59):
            resultado = self.detector.detectar(
                self.criar_transacao(segundos=segundo)
            )

        self.assertIn("TEMPO_60s", resultado)

    def test_quatro_transacoes_em_exatamente_60_segundos(self):
        resultado = []

        for segundo in (0, 10, 20, 60):
            resultado = self.detector.detectar(
                self.criar_transacao(segundos=segundo)
            )

        self.assertNotIn("TEMPO_60s", resultado)

    def test_cidades_diferentes_em_menos_de_10_minutos(self):
        self.detector.detectar(
            self.criar_transacao(segundos=0, cidade="Crato")
        )

        resultado = self.detector.detectar(
            self.criar_transacao(segundos=599, cidade="Fortaleza")
        )

        self.assertIn("GEO_10m", resultado)

    def test_cidades_diferentes_em_exatamente_10_minutos(self):
        self.detector.detectar(
            self.criar_transacao(segundos=0, cidade="Crato")
        )

        resultado = self.detector.detectar(
            self.criar_transacao(segundos=600, cidade="Fortaleza")
        )

        self.assertNotIn("GEO_10m", resultado)

    def test_mesma_cidade_nao_aciona_regra_geografica(self):
        self.detector.detectar(
            self.criar_transacao(segundos=0, cidade="Crato")
        )

        resultado = self.detector.detectar(
            self.criar_transacao(segundos=60, cidade="Crato")
        )

        self.assertNotIn("GEO_10m", resultado)

    def test_historico_separado_por_cliente(self):
        self.detector.detectar(
            self.criar_transacao(
                segundos=0, cliente="CLI001", cidade="Crato"
            )
        )

        resultado = self.detector.detectar(
            self.criar_transacao(
                segundos=10, cliente="CLI002", cidade="Fortaleza"
            )
        )

        self.assertNotIn("GEO_10m", resultado)
        self.assertNotIn("TEMPO_60s", resultado)


if __name__ == "__main__":
    unittest.main(verbosity=2)
