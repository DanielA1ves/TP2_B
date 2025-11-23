# TP2-B - Sistema de Integração gRPC + XML-RPC

## Dataset
Este projeto utiliza o dataset **Global House Purchase Decision** do Kaggle.

### Download do Dataset
1. Acede ao link: [Global House Purchase Decision Dataset](https://www.kaggle.com/datasets/mohankrishnathalla/global-house-purchase-decision-dataset)
2. Faz download do ficheiro `global_house_purchase_dataset.csv` e coloca-o na raiz do projeto.

> Nota: o ficheiro CSV não está incluído no repositório devido ao tamanho (>100MB).

## Arquitetura atual
- **Servidor único** (`server`):
  - gRPC na porta `50051` com métodos `UploadData`, `CountRecords`, `GetRecordByID`, `ExecuteXPath`.
  - XML-RPC na porta `8000` com `count_records`, `get_record_by_id`, `execute_xpath`.
  - Ambos partilham o mesmo XML carregado em memória/disco.
- **Cliente** (`client`):
  - Gera `house_purchase.xml` e `house_purchase.xsd` a partir do CSV, valida e faz upload via gRPC.
  - Corre testes gRPC: contagem, registo por ID=1, e XPath das primeiras cidades.

## Como correr com Docker Compose
1. (Opcional) limpar containers órfãos: `docker compose down --remove-orphans`
2. Subir e testar: `docker compose up --build`
   - O cliente termina após gerar/validar, enviar e testar.
   - O servidor fica ativo nas portas 50051 (gRPC) e 8000 (XML-RPC).

### Consultar manualmente
- gRPC: usar `client.py` ou outro cliente/stub apontando para `localhost:50051`.
- XML-RPC: `xmlrpc.client.ServerProxy('http://localhost:8000')` e chamar `count_records`, `get_record_by_id`, `execute_xpath`.

## Ficheiros principais
- `server.py` — servidor combinado gRPC + XML-RPC.
- `client.py` — gera/valida XML/XSD, envia via `UploadData` e testa.
- `xml_converter.py`, `schema_creator.py`, `validator.py` — geração e validação do XML/XSD a partir do CSV.
- `property_service.proto` e artefactos gerados `property_service_pb2*.py` — contratos gRPC.

