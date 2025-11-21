# Usa uma imagem base Python estável e otimizada
FROM python:3.11-slim

# Define o diretório de trabalho dentro do container
WORKDIR /app

# Copia o ficheiro de requisitos
COPY requirements.txt .

# Instalação das dependências do sistema e Python (essencial para lxml/etree)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2-dev \
    libxslt1-dev \
    gcc \
    # Instala as dependências Python
    && pip install --no-cache-dir -r requirements.txt \
    # Limpa pacotes de desenvolvimento para manter a imagem pequena
    && apt-get purge -y libxml2-dev libxslt1-dev gcc \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# Copia todos os ficheiros do projeto, incluindo o client.py corrigido
COPY . /app