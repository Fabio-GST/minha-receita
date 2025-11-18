-- Query SQL simples para validação rápida dos sócios MEI
-- Execute esta query para ver uma visão geral dos dados salvos

-- Visão geral dos sócios MEI inseridos
SELECT 
    'Total de sócios MEI' as metrica,
    COUNT(*)::text as valor
FROM socios_cnpj
WHERE qualificacao = 'Sócio Administrador'

UNION ALL

SELECT 
    'Total de empresas com sócios MEI' as metrica,
    COUNT(DISTINCT business_id)::text as valor
FROM socios_cnpj
WHERE qualificacao = 'Sócio Administrador'

UNION ALL

SELECT 
    'CPFs válidos (11 dígitos)' as metrica,
    COUNT(*)::text as valor
FROM socios_cnpj
WHERE qualificacao = 'Sócio Administrador'
  AND LENGTH(cpf_socio) = 11

UNION ALL

SELECT 
    'CPFs inválidos ou NULL' as metrica,
    COUNT(*)::text as valor
FROM socios_cnpj
WHERE qualificacao = 'Sócio Administrador'
  AND (LENGTH(cpf_socio) != 11 OR cpf_socio IS NULL)

UNION ALL

SELECT 
    'Nomes truncados (terminam com ...)' as metrica,
    COUNT(*)::text as valor
FROM socios_cnpj
WHERE qualificacao = 'Sócio Administrador'
  AND nome_socio LIKE '%...';

-- Exemplos de registros salvos
SELECT 
    sc.cnpj,
    b.razao_social as razao_social_empresa,
    sc.nome_socio,
    sc.cpf_socio,
    sc.qualificacao,
    CASE 
        WHEN LENGTH(sc.cpf_socio) = 11 THEN '✓ Válido'
        ELSE '✗ Inválido'
    END as status_cpf,
    CASE 
        WHEN sc.nome_socio LIKE '%...' THEN 'Truncado'
        ELSE 'OK'
    END as status_nome
FROM socios_cnpj sc
JOIN business b ON b.id = sc.business_id
WHERE sc.qualificacao = 'Sócio Administrador'
ORDER BY sc.id DESC
LIMIT 30;

