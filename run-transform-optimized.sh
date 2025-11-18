#!/bin/bash
# Script otimizado para executar transform com uso reduzido de memória

cd /root/minha-receita

# Carregar variáveis de ambiente
export $(cat .env | xargs)

# Configurar limite de memória do Go (6GB de 8GB disponíveis)
export GOMEMLIMIT=6GiB

# Executar com parâmetros otimizados para reduzir uso de memória
# -m 2: reduz workers paralelos de DB para 2
# -k 128: reduz writes paralelos KV para 128
# -b 1024: reduz batch size para 1024
nohup go run main.go transform --structured \
  --max-parallel-db-queries 2 \
  --max-parallel-kv-writes 128 \
  --batch-size 1024 \
  > transform.log 2>&1 &

echo "Processo iniciado em background (PID: $!)"
echo "Log sendo salvo em: /root/minha-receita/transform.log"
echo ""
echo "Para acompanhar o progresso:"
echo "  tail -f /root/minha-receita/transform.log"
echo ""
echo "Para verificar uso de memória:"
echo "  watch -n 5 'free -h && ps aux | grep \"go run\" | grep -v grep'"

