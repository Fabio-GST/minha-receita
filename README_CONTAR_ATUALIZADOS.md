# Como Contar Registros Atualizados

Este guia explica como contabilizar quantos registros da tabela `business` foram atualizados durante o processo de transformação.

## Métodos Rápidos

### 1. Usar o script bash (mais fácil)

```bash
cd /root/minha-receita
./contar_atualizados.sh
```

O script automaticamente:
- Carrega variáveis do `.env`
- Executa uma query simples
- Mostra total de registros e atualizações recentes

### 2. Query SQL direta (mais rápido)

```bash
cd /root/minha-receita
export $(cat .env | xargs)

psql $DATABASE_URL -c "
SELECT 
    COUNT(*) as total_registros,
    COUNT(CASE WHEN updated_at >= NOW() - INTERVAL '24 hours' THEN 1 END) as atualizados_24h,
    COUNT(CASE WHEN updated_at >= NOW() - INTERVAL '2 hours' THEN 1 END) as atualizados_2h,
    ROUND(
        COUNT(CASE WHEN updated_at >= NOW() - INTERVAL '24 hours' THEN 1 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 
        2
    ) as percentual_24h
FROM public.business;
"
```

## Queries Disponíveis

### Query 1: Total de atualizados (se tiver campo `created_at`)

```sql
SELECT 
    COUNT(*) as total_atualizados,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM public.business) as percentual_atualizados
FROM public.business
WHERE updated_at != created_at;
```

### Query 2: Atualizados nas últimas 24 horas

```sql
SELECT 
    COUNT(*) as atualizados_24h,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM public.business) as percentual
FROM public.business
WHERE updated_at >= NOW() - INTERVAL '24 hours';
```

### Query 3: Atualizados nas últimas 2 horas (última execução)

```sql
SELECT 
    COUNT(*) as atualizados_2h,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM public.business) as percentual
FROM public.business
WHERE updated_at >= NOW() - INTERVAL '2 hours';
```

### Query 4: Comparação Novos vs Atualizados (se tiver `created_at`)

```sql
SELECT 
    COUNT(*) as total_registros,
    COUNT(CASE WHEN created_at = updated_at THEN 1 END) as registros_novos,
    COUNT(CASE WHEN created_at != updated_at THEN 1 END) as registros_atualizados,
    ROUND(COUNT(CASE WHEN created_at != updated_at THEN 1 END) * 100.0 / COUNT(*), 2) as percentual_atualizados
FROM public.business;
```

### Query 5: Histograma por hora (últimas 24h)

```sql
SELECT 
    DATE_TRUNC('hour', updated_at) as hora,
    COUNT(*) as total_atualizados
FROM public.business
WHERE updated_at >= NOW() - INTERVAL '24 hours'
GROUP BY DATE_TRUNC('hour', updated_at)
ORDER BY hora DESC;
```

### Query 6: Últimos 10 registros atualizados

```sql
SELECT 
    cnpj,
    razao_social,
    updated_at,
    CASE 
        WHEN created_at != updated_at THEN 'ATUALIZADO'
        ELSE 'NOVO'
    END as status
FROM public.business
ORDER BY updated_at DESC
LIMIT 10;
```

## Arquivos Disponíveis

1. **`contar_atualizados.sql`** - Arquivo completo com todas as queries
2. **`contar_atualizados_simples.sql`** - Versão simplificada com verificação automática
3. **`contar_atualizados.sh`** - Script bash para execução rápida

## Executar Arquivo SQL Completo

```bash
cd /root/minha-receita
export $(cat .env | xargs)
psql $DATABASE_URL -f contar_atualizados.sql
```

## Explicação dos Campos

- **`updated_at`**: Campo que é atualizado sempre que há um `ON CONFLICT DO UPDATE`
- **`created_at`**: Campo que armazena quando o registro foi criado pela primeira vez (pode não existir)

## Como Funciona

Durante o processo de transformação:

1. **Novos registros**: Quando um CNPJ não existe, é inserido e `created_at = updated_at`
2. **Registros atualizados**: Quando um CNPJ já existe, o `ON CONFLICT DO UPDATE` atualiza os campos e define `updated_at = NOW()`

Portanto:
- Se `created_at = updated_at` → registro novo
- Se `created_at != updated_at` → registro atualizado
- Se não houver `created_at`, use `updated_at` recente como indicador

## Exemplo de Saída

```
 total_registros | atualizados_24h | atualizados_2h | percentual_24h 
-----------------+-----------------+-----------------+----------------
        68448345 |        1234567 |          234567 |          18.02
```

## Dicas

- **Durante a execução**: Use `updated_at >= NOW() - INTERVAL '2 hours'` para ver progresso recente
- **Após completar**: Use `updated_at != created_at` para ver total de atualizados
- **Monitoramento**: Execute periodicamente para acompanhar o progresso

