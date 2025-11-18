# Limpeza de Espaço em Disco

## Problema Identificado

O disco está **100% cheio** (apenas 27MB livres), causando:
- Erro SIGBUS no Badger durante compactação
- Falha no processo de transformação
- Impossibilidade de criar novos arquivos temporários

## Causa Raiz

Há **mais de 61GB** de diretórios temporários do Badger que não foram removidos em `/tmp`:
- 6 diretórios antigos de execuções anteriores
- Cada diretório pode ter de 923MB a 12GB
- Total: ~61GB de espaço desperdiçado

## Solução Imediata

### 1. Limpar Diretórios Temporários Antigos

Execute o script de limpeza:

```bash
cd /root/minha-receita
./limpar_temporarios.sh
```

O script irá:
- Listar todos os diretórios temporários encontrados
- Mostrar o tamanho de cada um
- Pedir confirmação antes de remover
- Liberar ~61GB de espaço

### 2. Verificar Espaço Após Limpeza

```bash
df -h /
```

Você deve ver o espaço disponível aumentar significativamente.

## Prevenção Futura

### Melhorias Implementadas no Código

1. **Logs melhorados**: O código agora registra quando tenta remover diretórios temporários
2. **Tratamento de erros**: Avisa quando não consegue remover um diretório temporário

### Limpeza Manual Periódica

Execute periodicamente para manter o espaço livre:

```bash
# Verificar diretórios temporários
find /tmp -name "minha-receita-*" -type d -exec du -sh {} \;

# Limpar manualmente se necessário
./limpar_temporarios.sh
```

### Limpeza Automática (Opcional)

Você pode adicionar ao crontab para limpeza automática:

```bash
# Limpar diretórios temporários com mais de 1 dia
0 2 * * * find /tmp -name "minha-receita-*" -type d -mtime +1 -exec rm -rf {} \;
```

## Comandos Úteis

### Verificar Espaço em Disco
```bash
df -h /
```

### Verificar Diretórios Temporários
```bash
du -sh /tmp/minha-receita-* 2>/dev/null | sort -h
```

### Verificar Tamanho de Arquivos Grandes
```bash
find /root/minha-receita -type f -size +100M -exec ls -lh {} \;
```

### Verificar Logs do Sistema
```bash
journalctl --disk-usage
du -sh /var/log/*
```

## Após Limpeza

Depois de liberar espaço:

1. **Reinicie o processo de transformação** com os parâmetros otimizados:
   ```bash
   ./run-transform-optimized.sh
   ```

2. **Monitore o espaço** durante a execução:
   ```bash
   watch -n 10 'df -h /'
   ```

3. **Limpe temporários** se necessário durante a execução

## Notas Importantes

- ⚠️ **NÃO remova** diretórios temporários de execuções em andamento
- ✅ **Remova apenas** diretórios de execuções antigas (mais de algumas horas)
- 💡 O script `limpar_temporarios.sh` lista todos antes de remover
- 🔍 Use `--dry-run` para ver o que seria removido sem remover de fato

