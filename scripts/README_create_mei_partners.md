# Script: create_mei_partners

## Descrição

Script para processar o arquivo `empresas_sem_socios.csv` e criar registros de sócios MEI (Microempreendedor Individual) na tabela `socios_cnpj` quando a razão social contém um CPF no final.

## Funcionamento

O script:
1. Lê o arquivo CSV `empresas_sem_socios.csv`
2. Identifica casos onde a razão social termina com 11 dígitos (CPF)
3. Extrai o CPF e o nome do sócio (razão social sem o CPF)
4. Busca o `business_id` pelo CNPJ na tabela `business`
5. Insere registros na tabela `socios_cnpj` com:
   - `business_id`: ID da empresa
   - `cnpj`: CNPJ da empresa
   - `nome_socio`: Nome extraído da razão social (máx 120 caracteres)
   - `cpf_socio`: CPF extraído (11 dígitos)
   - `data_entrada_sociedade`: NULL
   - `qualificacao`: "Sócio Administrador"

## Requisitos

- Variável de ambiente `DATABASE_URL` configurada
- Variável de ambiente `POSTGRES_SCHEMA` (opcional, padrão: `public`)
- Arquivo `/root/minha-receita/empresas_sem_socios.csv` existente

## Compilação

```bash
cd /root/minha-receita/scripts
go build -o create_mei_partners create_mei_partners.go
```

## Execução

### Opção 1: Usando arquivo .env (Recomendado)

O projeto já possui um arquivo `.env` na raiz. Use o script wrapper:

```bash
cd /root/minha-receita
./scripts/run-create-mei-partners.sh
```

O script automaticamente carrega as variáveis do arquivo `.env`:
- `DATABASE_URL`: URL de conexão com o PostgreSQL
- `POSTGRES_SCHEMA`: Schema do banco (opcional, padrão: `public`)

### Opção 2: Variáveis de ambiente diretas

```bash
export DATABASE_URL="postgres://usuario:senha@host:porta/banco"
export POSTGRES_SCHEMA="public"  # opcional

cd /root/minha-receita/scripts
./create_mei_partners
```

### Opção 3: Carregar .env manualmente

```bash
cd /root/minha-receita
export $(cat .env | grep -v '^#' | xargs)
cd scripts
./create_mei_partners
```

## Exemplo de Processamento

**Entrada (CSV):**
```
id,cnpj,razao_social,telefones
61957482,22931048000156,LUCIANO JOSE DA SILVA 06017523414,8288668453
```

**Saída (tabela socios_cnpj):**
```
business_id: 12345
cnpj: 22931048000156
nome_socio: LUCIANO JOSE DA SILVA
cpf_socio: 06017523414
data_entrada_sociedade: NULL
qualificacao: Sócio Administrador
```

## Estatísticas

O script exibe estatísticas durante e ao final do processamento:
- Total de linhas processadas
- CPFs encontrados na razão social
- Empresas encontradas no banco
- Registros inseridos com sucesso
- Erros encontrados

## Batch Processing

- Processa em batches de 10.000 registros por transação
- Otimizado para performance com `synchronous_commit = OFF`
- Continua processamento mesmo com erros individuais

## Medidor de Progresso

O script exibe uma barra de progresso visual durante o processamento que mostra:
- Descrição: "Processando empresas"
- Contador de linhas processadas
- Estatísticas atualizadas: total processado, CPFs encontrados, inseridos e erros
- Tempo decorrido
- Barra visual de progresso

Exemplo de saída:
```
Processando: 50000 processadas | 12000 CPFs | 11500 inseridos | 50 erros [=====>    ] 45% 22500/50000
```

## Logs

O script usa `slog` para logging estruturado e exibe:
- Informações iniciais (header do CSV, conexão ao banco)
- Warnings para registros que falharam
- Estatísticas finais detalhadas ao concluir

