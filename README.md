# projeto_fia

Criar um datalake/data lakehouse com as 3 camadas de dados, onde os dados de entrada sejam armazenados da raw data, os dados tratados na camada trusted e os dados de negócio na camada business.


### Passo a Passo

Executar docker-compose up --build

MinIO e api_loader irão subir, o api_loader já sobe fazendo a ingestão no entanto para isso é necessário passar o token dentro do docker-compose.

Os demais secrets mantive pois é um secret rodando local evitando assim precisar criar um novo access key no MinIO.

Foi criado pasta minio -> data, o container do minio aponta para esse diretório do host, logo ao clonar os dados conforme forem upados para o Repos ficaram persistentes ao rodar local, da mesma forma o api_loader aponta para diretório local do host api_loader -> main.py
