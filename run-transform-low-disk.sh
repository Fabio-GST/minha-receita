#!/bin/bash
# Script otimizado para executar transform com MÍNIMO uso de espaço em disco
# Use este script quando tiver muito pouco espaço disponível (< 30GB)

cd /root/minha-receita

# Carregar variáveis de ambiente
export $(cat .env | xargs)

# Configurar limite de memória do Go (reduzido para deixar mais espaço para o PostgreSQL)
export GOMEMLIMIT=4GiB

# Parâmetros ULTRA-OTIMIZADOS para espaço mínimo:
# -m 1: apenas 1 query paralela ao DB (evita múltiplas transações grandes)
# -k 32: writes paralelos KV reduzidos ao mínimo (reduz memória e I/O)
# -b 128: batch size mínimo (128 registros por transação)
#         Transações muito pequenas = mínimo espaço temporário
#         Commits muito frequentes = libera espaço constantemente
echo "Iniciando transform com parâmetros otimizados para espaço mínimo..."
echo "Parâmetros: --max-parallel-db-queries 1 --max-parallel-kv-writes 32 --batch-size 128"
echo ""

nohup go run main.go transform --structured \
  --max-parallel-db-queries 1 \
  --max-parallel-kv-writes 32 \
  --batch-size 128 \
  > transform.log 2>&1 &

PID=$!
echo "Processo iniciado em background (PID: $PID)"
echo "Log sendo salvo em: /root/minha-receita/transform.log"
echo ""
echo "Para acompanhar o progresso:"
echo "  tail -f /root/minha-receita/transform.log"
echo ""
echo "Para verificar processos Go rodando simultaneamente:"
echo "  ps aux | grep -E \"go run|go build\" | grep -v grep | wc -l"
echo "  ps aux | grep -E \"go run|go build\" | grep -v grep"
echo ""
echo "Para verificar uso de disco:"
echo "  watch -n 10 'df -h / && ps aux | grep \"go run\" | grep -v grep'"
echo ""
echo "Para verificar espaço usado pelo PostgreSQL:"
echo "  du -sh /var/lib/postgresql/* 2>/dev/null || du -sh \$(psql -t -c \"SHOW data_directory\" | xargs) 2>/dev/null"
echo ""
echo "Para parar o processo:"
echo "  kill $PID"

