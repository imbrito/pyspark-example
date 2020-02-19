# PySpark Example

Este projeto apresenta uma implementação `pyspark` resolvendo 5 questões a partir de um arquivo de log.

Fonte​ oficial​​ do​​ dateset: http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html

- [Jul​ 01​ to Jul​ 31,​ ASCII​ format,​ 20.7​ MB​ gzip compressed​, 205.2​ ​MB.](ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz)
- [Aug 04 to Aug 31, ASCII format, 21.8 MB gzip compressed, 167.8 MB.](ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz)


O dataset possui todas as requisições HTTP para o servidor da **NASA Kennedy Space​​ Center​ WWW​​ na​ Flórida​​** para​ um​​ período​​ específico.

Os​​ logs​ estão​​ em​​ arquivos​​ ASCII​ com​ uma​ linha​ por​ requisição​ com​ as​​ seguintes​​ colunas:

- **Host**: um hostname quando possível, caso contrário o endereço de internet se o nome não​ ​puder​ ​ser​ ​identificado;
- **Timestamp**:​ no​ formato​​ "DIA/MÊS/ANO:HH:MM:SS​ ​TIMEZONE";
- **Request​**: método HTTP e URL;
- **HTTP Code**: código​ ​do​ ​retorno​ ​HTTP;
- **Bytes**: total​ de​​ bytes​ retornados.

## Setup

1. Python >= 3.7.1
2. Virtualenv >= 16.4.3
3. Wget >= 1.17.1

## Deploy

1. clone o presente repositório: `$ git clone git@github.com:imbrito/pyspark-example.git`.
2. acesse a pasta do projeto: `$ cd pyspark-example`.
3. faça download dos aqruivos de entrada: `$ make wget`.
4. instale as dependências: `$ make install`.
5. ative o ambiente: `$ source venv/bin/activate`.
6. execute o pipeline: `$ python run.py`.
