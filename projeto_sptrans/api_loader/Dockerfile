FROM python:3.10-slim

WORKDIR /usr/src/app

COPY requirements.txt ./

RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o código fonte para o container
COPY . .

# Executar a aplicação
CMD ["python", "-u", "main.py"]

