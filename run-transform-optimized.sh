#!/bin/bash
# Script otimizado para executar transform com uso reduzido de espaço em disco
# Otimizado para ambientes com espaço limitado (ex: 23GB disponíveis)

cd /root/minha-receita

# Carregar variáveis de ambiente
export $(cat .env | xargs)

# Configurar limite de memória do Go (6GB de 8GB disponíveis)
export GOMEMLIMIT=6GiB

# Executar com parâmetros otimizados para reduzir uso de espaço em disco:
# -m 1: apenas 1 query paralela ao DB (evita múltiplas transações grandes simultâneas)
#       Reduz uso de espaço temporário durante transações
# -k 64: reduz writes paralelos KV para 64 (suficiente para HDD/SSD, reduz memória)
# -b 256: batch size reduzido para 256 registros (menos registros por transação)
#         Transações menores = menos espaço temporário necessário
#         Commits mais frequentes = libera espaço mais rápido
nohup go run main.go transform --structured \
  --max-parallel-db-queries 1 \
  --max-parallel-kv-writes 64 \
  --batch-size 256 \
  > transform.log 2>&1 &

echo "Processo iniciado em background (PID: $!)"
echo "Log sendo salvo em: /root/minha-receita/transform.log"
echo ""
echo "Para acompanhar o progresso:"
echo "  tail -f /root/minha-receita/transform.log"
echo ""
echo "Para verificar uso de memória:"
echo "  watch -n 5 'free -h && ps aux | grep \"go run\" | grep -v grep'"

