-- Script para corrigir CPFs dos sócios após o processamento completo
-- Este script extrai apenas os dígitos dos CPFs mascarados/parciais
-- e atualiza os registros que estão com NULL mas têm dados no JSON original

-- IMPORTANTE: Execute este script APENAS após o processamento completo
-- Ajuste o schema conforme necessário (padrão: 'public')

-- Opção 1: Se os dados estão na tabela socios_cnpj e você tem acesso ao JSON original
-- (Caso esteja usando modo estruturado mas ainda tenha o JSON disponível)

-- Opção 2: Se você precisa reprocessar apenas os sócios da tabela socios_cnpj
-- e tem acesso aos dados originais do Badger/KV store, será necessário
-- um script Go para ler e atualizar

-- Opção 3: UPDATE direto se você tem os dados em outra tabela/coluna
-- Exemplo (ajuste conforme sua estrutura):

/*
-- Exemplo de UPDATE direto (se os dados estão em outra coluna/tabela)
UPDATE socios_cnpj 
SET cpf_socio = REGEXP_REPLACE(
    COALESCE(cpf_original, ''), 
    '[^0-9]', 
    '', 
    'g'
)
WHERE cpf_socio IS NULL 
  AND cpf_original IS NOT NULL 
  AND REGEXP_REPLACE(COALESCE(cpf_original, ''), '[^0-9]', '', 'g') != '';
*/

-- NOTA: Como os dados estão sendo salvos diretamente na tabela estruturada,
-- e o problema foi na inserção inicial, você tem duas opções:

-- OPÇÃO RECOMENDADA: Aguardar finalizar e rodar um script Go que:
-- 1. Lê os dados originais do Badger/KV store
-- 2. Extrai os dígitos dos CPFs
-- 3. Atualiza a tabela socios_cnpj

-- Script Go será criado em: scripts/fix_cpf_socios.go

