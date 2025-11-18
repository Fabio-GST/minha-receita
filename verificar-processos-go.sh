#!/bin/bash
# Script para verificar processos Go rodando simultaneamente

echo "=== Processos Go rodando simultaneamente ==="
echo ""

# Contar processos
TOTAL=$(ps aux | grep -E "go run|go build" | grep -v grep | wc -l)

if [ "$TOTAL" -eq 0 ]; then
    echo "Nenhum processo Go encontrado rodando."
    exit 0
fi

echo "Total de processos: $TOTAL"
echo ""

# Listar processos com detalhes
ps aux | grep -E "go run|go build" | grep -v grep | awk '{
    printf "PID: %-8s | Iniciado: %-8s | CPU: %5s%% | Mem: %5s%%\n", $2, $9, $3, $4
    cmd = ""
    for (i=11; i<=NF; i++) {
        cmd = cmd " " $i
    }
    print "  Comando:" cmd
    print ""
}'

echo "Para matar todos os processos Go:"
echo "  pkill -f 'go run'"
echo ""
echo "Para matar um processo específico:"
echo "  kill <PID>"

