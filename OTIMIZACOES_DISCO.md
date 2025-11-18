# Otimizações para Espaço em Disco Limitado

Este documento explica as otimizações implementadas e como usar os parâmetros para reduzir o uso de espaço em disco durante o processo de transformação.

## Problema

Quando há pouco espaço em disco disponível (ex: 23GB), o PostgreSQL pode falhar ao tentar estender arquivos durante operações de bulk loading. O erro típico é:

```
ERROR: could not extend file "base/16514/49725.10": wrote only 4096 of 8192 bytes at block 1431923 (SQLSTATE 53100)
```

## Otimizações Implementadas no Código

### 1. PreLoad Resiliente
- O código continua mesmo se `ALTER TABLE SET UNLOGGED` falhar por falta de espaço
- `UNLOGGED` é uma otimização, não um requisito obrigatório

### 2. Redução de Memória Temporária
- `work_mem` reduzido de 256MB para 64MB por transação
- `maintenance_work_mem` definido para 128MB
- Reduz significativamente o uso de espaço temporário

### 3. Batch Size Padrão Reduzido
- Batch size padrão reduzido de 2048 para 512 registros
- Transações menores = menos espaço temporário necessário

### 4. VACUUM Otimizado
- `VACUUM ANALYZE` executado antes de iniciar o carregamento
- Libera espaço antes de começar

## Parâmetros de Linha de Comando

### Parâmetros Principais

#### `--max-parallel-db-queries` (-m)
**O que faz:** Controla quantas queries paralelas são executadas simultaneamente ao banco de dados.

**Impacto no espaço:**
- Valores maiores (2-4) = múltiplas transações grandes simultâneas = mais espaço temporário necessário
- Valor menor (1) = uma transação por vez = mínimo espaço temporário

**Recomendações:**
- **Espaço limitado (< 30GB):** Use `1`
- **Espaço moderado (30-50GB):** Use `1-2`
- **Espaço adequado (> 50GB):** Use `2-4`

#### `--batch-size` (-b)
**O que faz:** Define quantos registros são processados em cada transação.

**Impacto no espaço:**
- Valores maiores (1024-2048) = transações grandes = mais espaço temporário necessário
- Valores menores (128-256) = transações pequenas = menos espaço temporário, commits mais frequentes

**Recomendações:**
- **Espaço muito limitado (< 20GB):** Use `128`
- **Espaço limitado (20-30GB):** Use `256`
- **Espaço moderado (30-50GB):** Use `512`
- **Espaço adequado (> 50GB):** Use `1024-2048`

#### `--max-parallel-kv-writes` (-k)
**O que faz:** Controla quantas escritas paralelas são feitas no key-value store (Badger).

**Impacto no espaço:**
- Valores maiores = mais memória e I/O = pode competir com PostgreSQL por recursos
- Valores menores = menos memória, mas pode ser mais lento

**Recomendações:**
- **Espaço muito limitado:** Use `32`
- **Espaço limitado:** Use `64`
- **Espaço adequado:** Use `128-256`

## Scripts Disponíveis

### 1. `run-transform-optimized.sh` (Recomendado para 20-30GB disponíveis)
```bash
./run-transform-optimized.sh
```

**Parâmetros:**
- `--max-parallel-db-queries 1`
- `--max-parallel-kv-writes 64`
- `--batch-size 256`

### 2. `run-transform-low-disk.sh` (Para < 20GB disponíveis)
```bash
./run-transform-low-disk.sh
```

**Parâmetros:**
- `--max-parallel-db-queries 1`
- `--max-parallel-kv-writes 32`
- `--batch-size 128`

## Comando Manual Otimizado

Para seu caso específico (23GB disponíveis), use:

```bash
cd /root/minha-receita
export $(cat .env | xargs)
export GOMEMLIMIT=4GiB  # Reduzido para deixar mais espaço para PostgreSQL

nohup go run main.go transform --structured \
  --max-parallel-db-queries 1 \
  --max-parallel-kv-writes 64 \
  --batch-size 256 \
  > transform.log 2>&1 &
```

## Comparação de Parâmetros

| Espaço Disponível | max-parallel-db-queries | max-parallel-kv-writes | batch-size | Script |
|-------------------|------------------------|------------------------|------------|--------|
| < 20GB            | 1                      | 32                     | 128        | low-disk |
| 20-30GB           | 1                      | 64                     | 256        | optimized |
| 30-50GB           | 1-2                    | 64-128                 | 512        | manual |
| > 50GB            | 2-4                    | 128-256                 | 1024-2048  | manual |

## Monitoramento

### Verificar espaço em disco:
```bash
watch -n 10 'df -h /'
```

### Acompanhar o log:
```bash
tail -f /root/minha-receita/transform.log
```

### Verificar processos:
```bash
ps aux | grep "go run" | grep -v grep
```

### Verificar espaço usado pelo PostgreSQL:
```bash
# Se PostgreSQL está em /var/lib/postgresql
du -sh /var/lib/postgresql/*

# Ou descobrir o data_directory:
psql -t -c "SHOW data_directory" | xargs du -sh
```

## Dicas Adicionais

1. **Execute VACUUM manualmente antes de iniciar:**
   ```sql
   VACUUM ANALYZE business, socios_cnpj;
   ```

2. **Monitore o espaço durante a execução:**
   - Se o espaço estiver acabando rapidamente, pare o processo
   - Reduza ainda mais o `batch-size` (ex: 64 ou 128)
   - Execute `VACUUM` novamente e continue

3. **Se o processo falhar por espaço:**
   - Execute `VACUUM` para liberar espaço
   - Reduza os parâmetros ainda mais
   - Reinicie o processo

4. **Lembre-se:** Com `ON CONFLICT DO UPDATE`, muitos registros serão apenas atualizações, não inserções novas, então o crescimento do banco será menor do que o esperado.

