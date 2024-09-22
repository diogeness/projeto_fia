FROM apache/spark-py:v3.4.0

# Definir o diretório de trabalho
WORKDIR /usr/src/app

# Copiar os requirements e instalar as dependências
COPY requirements.txt ./
USER root
RUN pip install --upgrade pip && pip install -r requirements.txt

# Garantir que o Java esteja configurado corretamente para o Spark
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$JAVA_HOME/bin:$PATH

# Copiar todo o código da aplicação
COPY . .

# Comando para rodar o script Python usando python3
CMD ["python3", "-u", "main.py"]
