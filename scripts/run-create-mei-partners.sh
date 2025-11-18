#!/bin/bash
# Script para executar create_mei_partners com variáveis de ambiente carregadas do .env

cd /root/minha-receita

# Carregar variáveis de ambiente do arquivo .env
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
else
    echo "Aviso: Arquivo .env não encontrado. Usando variáveis de ambiente do sistema."
fi

# Executar o script
cd scripts
./create_mei_partners "$@"

