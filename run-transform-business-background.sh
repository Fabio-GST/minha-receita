#!/bin/bash
# Script para executar transform em background salvando na tabela business

cd /root/minha-receita

# Carregar variáveis de ambiente do arquivo .env
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
else
    echo "Aviso: Arquivo .env não encontrado. Usando variáveis de ambiente do sistema."
fi

# Configurar limite de memória do Go (6GB de 8GB disponíveis)
export GOMEMLIMIT=6GiB

# Verificar se já existe um processo rodando
if pgrep -f "go run main.go transform --structured" > /dev/null; then
    echo "⚠️  Já existe um processo de transform rodando!"
    echo "PIDs encontrados:"
    pgrep -f "go run main.go transform --structured"
    echo ""
    echo "Para parar o processo existente:"
    echo "  pkill -f 'go run main.go transform --structured'"
    echo ""
    read -p "Deseja continuar mesmo assim? (s/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Ss]$ ]]; then
        exit 1
    fi
fi

# Executar em background com nohup
echo "🚀 Iniciando transform em background..."
echo "📝 Logs serão salvos em: /root/minha-receita/transform-business.log"
echo ""

nohup go run main.go transform --structured \
  --max-parallel-db-queries 1 \
  --max-parallel-kv-writes 64 \
  --batch-size 256 \
  > transform-business.log 2>&1 &

PID=$!

echo "✅ Processo iniciado em background (PID: $PID)"
echo ""
echo "📊 Para acompanhar o progresso:"
echo "   tail -f /root/minha-receita/transform-business.log"
echo ""
echo "🔍 Para verificar se está rodando:"
echo "   ps aux | grep 'go run main.go transform' | grep -v grep"
echo ""
echo "📈 Para verificar uso de recursos:"
echo "   watch -n 5 'free -h && ps aux | grep \"go run\" | grep -v grep'"
echo ""
echo "🛑 Para parar o processo:"
echo "   pkill -f 'go run main.go transform --structured'"
echo ""

