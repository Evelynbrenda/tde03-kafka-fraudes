# TDE 03 — Detecção de Fraudes com Apache Kafka

Projeto acadêmico de detecção de fraudes em transações financeiras simuladas, utilizando processamento de mensagens com Apache Kafka, Python e armazenamento em SQLite.

## Tecnologias utilizadas

- **Python:** implementação do produtor, consumidor e regras de detecção.
- **Apache Kafka:** transmissão das transações entre os componentes.
- **Docker Compose:** execução do Kafka em container.
- **SQLite:** armazenamento das transações e dos resultados da detecção.
- **unittest:** testes automatizados das regras de fraude.

## Arquitetura do projeto

1. O produtor gera transações simuladas periodicamente.
2. As mensagens são publicadas no tópico `transacoes` do Kafka.
3. O consumidor recebe as mensagens e aplica as regras de detecção.
4. Todas as transações processadas são armazenadas no banco SQLite.
5. Quando uma regra é acionada, o consumidor exibe um alerta no terminal.

## Regras de detecção

| Código | Regra |
|---|---|
| `ALTO_VALOR` | Transação com valor igual ou superior a R$ 10.000,00. |
| `TEMPO_60s` | Quatro ou mais transações do mesmo cliente em menos de 60 segundos. |
| `GEO_10m` | Transações do mesmo cliente em cidades diferentes dentro de menos de 10 minutos. |

As regras identificam comportamentos suspeitos em dados simulados. Um alerta não comprova, por si só, a ocorrência de fraude real.

## Pré-requisitos

- Python instalado.
- Docker Desktop em execução.
- Git.

## Instalação e execução

### 1. Clonar o repositório

```bash
git clone URL_DO_REPOSITORIO
cd tde03-kafka-fraudes
```

Substitua `URL_DO_REPOSITORIO` pelo endereço real do repositório Git.

### 2. Criar e ativar o ambiente virtual

No Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 3. Instalar as dependências

```powershell
pip install -r requirements.txt
```

### 4. Iniciar o Kafka

Na pasta do projeto:

```powershell
docker compose up -d
```

O Kafka executado pelo Docker fica disponível na porta `9093` do computador. Essa configuração permite coexistir com uma instalação local do Kafka que utiliza a porta `9092`.

### 5. Executar o consumidor

Abra um terminal com o ambiente virtual ativado e execute:

```powershell
python consumer.py
```

O consumidor inicializa o banco de dados, conecta-se ao Kafka e começa a processar as transações.

### 6. Executar o produtor

Em outro terminal, na mesma pasta do projeto, ative o ambiente virtual e execute:

```powershell
.\.venv\Scripts\Activate.ps1
python producer.py
```

O produtor passa a enviar transações simuladas para o tópico `transacoes`.

## Banco de dados

As transações processadas são armazenadas em `transacoes.db`, utilizando SQLite.

O banco é criado localmente durante a execução e não precisa ser versionado no Git.

## Testes automatizados

Para executar os testes das regras de detecção:

```powershell
python -m unittest discover -s testes -v
```

Os testes verificam os limites de valor, as janelas de tempo, as diferenças entre cidades e o isolamento do histórico por cliente.

## Encerramento

Para interromper o produtor ou consumidor, pressione `Ctrl+C` no respectivo terminal.

Para parar o Kafka executado pelo Docker:

```powershell
docker compose down
```

Esse comando para e remove o container, mas preserva o volume de dados nomeado do Kafka.

## Estrutura principal

```text
tde03-kafka-fraudes/
├── consumer.py
├── producer.py
├── database.py
├── docker-compose.yml
├── requirements.txt
├── README.md
└── testes/
    └── test_fraudes.py
```
