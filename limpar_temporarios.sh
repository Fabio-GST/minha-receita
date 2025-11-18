#!/bin/bash
# Script para limpar diretórios temporários do Badger que não foram removidos
# Uso: ./limpar_temporarios.sh [--dry-run]

DRY_RUN=false
if [ "$1" == "--dry-run" ]; then
    DRY_RUN=true
    echo "MODO DRY-RUN: Nenhum arquivo será removido"
fi

echo "=========================================="
echo "Limpando diretórios temporários do Badger"
echo "=========================================="
echo ""

# Encontra todos os diretórios temporários
TEMP_DIRS=$(find /tmp -maxdepth 1 -type d -name "minha-receita-*" 2>/dev/null)

if [ -z "$TEMP_DIRS" ]; then
    echo "Nenhum diretório temporário encontrado."
    exit 0
fi

TOTAL_SIZE=0
COUNT=0

echo "Diretórios encontrados:"
echo "----------------------"
for dir in $TEMP_DIRS; do
    if [ -d "$dir" ]; then
        SIZE_STR=$(du -sh "$dir" 2>/dev/null | cut -f1)
        SIZE=$(du -sb "$dir" 2>/dev/null | cut -f1)
        TOTAL_SIZE=$((TOTAL_SIZE + SIZE))
        COUNT=$((COUNT + 1))
        echo "[$COUNT] $dir - $SIZE_STR"
    fi
done

TOTAL_MB=$((TOTAL_SIZE / 1024 / 1024))
TOTAL_GB=$((TOTAL_MB / 1024))
TOTAL_MB_REMAINDER=$((TOTAL_MB % 1024))

echo ""
if [ $TOTAL_GB -gt 0 ]; then
    echo "Total: $COUNT diretórios, ${TOTAL_GB}.${TOTAL_MB_REMAINDER}GB (~${TOTAL_MB}MB)"
else
    echo "Total: $COUNT diretórios, ~${TOTAL_MB}MB"
fi
echo ""

if [ "$DRY_RUN" == "true" ]; then
    echo "DRY-RUN: Nenhum arquivo foi removido."
    exit 0
fi

read -p "Deseja remover todos esses diretórios? (s/N): " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Ss]$ ]]; then
    echo "Operação cancelada."
    exit 0
fi

echo ""
echo "Removendo diretórios..."
for dir in $TEMP_DIRS; do
    if [ -d "$dir" ]; then
        echo "Removendo: $dir"
        rm -rf "$dir" 2>/dev/null
        if [ $? -eq 0 ]; then
            echo "  ✓ Removido com sucesso"
        else
            echo "  ✗ Erro ao remover"
        fi
    fi
done

echo ""
echo "=========================================="
echo "Limpeza concluída!"
if [ $TOTAL_GB -gt 0 ]; then
    echo "Espaço liberado: ~${TOTAL_GB}.${TOTAL_MB_REMAINDER}GB"
else
    echo "Espaço liberado: ~${TOTAL_MB}MB"
fi
echo "=========================================="

# Mostra espaço disponível agora
echo ""
echo "Espaço em disco agora:"
df -h / | tail -1

