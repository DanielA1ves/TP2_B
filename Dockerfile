# Usa uma imagem base Python estável e otimizada
FROM python:3.11-slim

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia o ficheiro de requisitos e instala as dependências
COPY requirements.txt .

# Instala bibliotecas de sistema necessárias para compilar o 'lxml' e depois as dependências Python
# O lxml precisa do libxml2 e libxslt
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2-dev \
    libxslt1-dev \
    gcc \
    # Instala as dependências Python
    && pip install --no-cache-dir -r requirements.txt \
    # Limpa os ficheiros de cache e as libs de desenvolvimento para manter a imagem pequena
    && apt-get purge -y libxml2-dev libxslt1-dev gcc \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Copia todos os ficheiros do seu projeto para o container
# Isto inclui os seus scripts, o CSV, o .proto, etc.
COPY . .

# Comando de entrada (opcional, pois será substituído pelo docker-compose)
# ENTRYPOINT ["python", "grpc_server.py"]