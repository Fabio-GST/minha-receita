-- Script para verificar quantos CPFs estão NULL
-- Execute este script para decidir se deve parar e recomeçar ou continuar

-- Verificar percentual de CPFs NULL
SELECT 
    COUNT(*) as total_socios,
    COUNT(*) FILTER (WHERE cpf_socio IS NULL) as cpf_null,
    COUNT(*) FILTER (WHERE cpf_socio IS NOT NULL) as cpf_preenchido,
    ROUND(100.0 * COUNT(*) FILTER (WHERE cpf_socio IS NULL) / COUNT(*), 2) as percentual_null,
    ROUND(100.0 * COUNT(*) FILTER (WHERE cpf_socio IS NOT NULL) / COUNT(*), 2) as percentual_preenchido
FROM socios_cnpj;

-- Verificar distribuição por empresa (quantas empresas têm sócios sem CPF)
SELECT 
    COUNT(DISTINCT business_id) as total_empresas,
    COUNT(DISTINCT business_id) FILTER (
        WHERE business_id IN (
            SELECT DISTINCT business_id 
            FROM socios_cnpj 
            WHERE cpf_socio IS NULL
        )
    ) as empresas_com_socios_sem_cpf,
    ROUND(100.0 * COUNT(DISTINCT business_id) FILTER (
        WHERE business_id IN (
            SELECT DISTINCT business_id 
            FROM socios_cnpj 
            WHERE cpf_socio IS NULL
        )
    ) / COUNT(DISTINCT business_id), 2) as percentual_empresas_afetadas
FROM socios_cnpj;

-- Verificar alguns exemplos de CPFs NULL
SELECT 
    cnpj,
    nome_socio,
    cpf_socio,
    qualificacao
FROM socios_cnpj 
WHERE cpf_socio IS NULL
LIMIT 10;

-- DECISÃO BASEADA NO RESULTADO:
-- Se percentual_null > 50% → PARAR E RECOMEÇAR
-- Se percentual_null < 50% → CONTINUAR + SCRIPT DE CORREÇÃO

