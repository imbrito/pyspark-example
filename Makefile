include local.env

.ONESHELL:
PYTHON := ${PWD}/venv/bin/python3
PIP := ${PWD}/venv/bin/pip3

wget:
	@echo "Gera o donwload dos arquivos de log"
	wget -P ${PWD}/data ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz
	wget -P ${PWD}/data ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz

venv:
	@echo "Inicializa uma venv local."
	virtualenv venv -p python3.7

install: venv
	@echo "Instala as dependências numa venv local."
	${PIP} install -r requirements.txt

clean: 
	@echo "Remove a venv local."
	sudo rm -rf venv data