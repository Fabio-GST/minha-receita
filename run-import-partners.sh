#!/bin/bash
# Script para executar import-partners com variáveis de ambiente carregadas

cd /root/minha-receita

# Carregar variáveis de ambiente do arquivo .env
export $(cat .env | xargs)

# Configurar limite de memória do Go (6GB de 8GB disponíveis)
export GOMEMLIMIT=6GiB

# Executar import-partners
go run main.go import-partners --directory data "$@"

