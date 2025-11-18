-- Script SQL para validar os sócios MEI criados pelo script create_mei_partners
-- Execute estas queries para verificar como os dados estão sendo salvos

-- 1. Contagem geral de sócios MEI inseridos
SELECT 
    COUNT(*) as total_socios_mei,
    COUNT(DISTINCT business_id) as total_empresas_com_socios_mei,
    COUNT(DISTINCT cnpj) as total_cnpjs_unicos
FROM socios_cnpj
WHERE qualificacao = 'Sócio Administrador';

-- 2. Verificar alguns exemplos de registros inseridos
SELECT 
    sc.id,
    sc.business_id,
    sc.cnpj,
    sc.nome_socio,
    sc.cpf_socio,
    sc.qualificacao,
    sc.data_entrada_sociedade,
    b.razao_social,
    b.nome_fantasia
FROM socios_cnpj sc
JOIN business b ON b.id = sc.business_id
WHERE sc.qualificacao = 'Sócio Administrador'
ORDER BY sc.id DESC
LIMIT 20;

-- 3. Verificar se há CPFs inválidos (não tem 11 dígitos)
SELECT 
    COUNT(*) as cpf_invalidos,
    cpf_socio,
    nome_socio,
    cnpj
FROM socios_cnpj
WHERE qualificacao = 'Sócio Administrador'
  AND (LENGTH(cpf_socio) != 11 OR cpf_socio IS NULL)
GROUP BY cpf_socio, nome_socio, cnpj
LIMIT 10;

-- 4. Verificar se há nomes truncados (mais de 120 caracteres originalmente)
-- Nota: Isso não pode ser detectado diretamente, mas podemos verificar se há muitos com "..."
SELECT 
    COUNT(*) as possivelmente_truncados,
    COUNT(*) * 100.0 / (SELECT COUNT(*) FROM socios_cnpj WHERE qualificacao = 'Sócio Administrador') as percentual
FROM socios_cnpj
WHERE qualificacao = 'Sócio Administrador'
  AND nome_socio LIKE '%...';

-- 5. Estatísticas por empresa (quantos sócios MEI cada empresa tem)
SELECT 
    b.cnpj,
    b.razao_social,
    COUNT(sc.id) as total_socios_mei,
    STRING_AGG(sc.nome_socio, ', ' ORDER BY sc.nome_socio) as nomes_socios
FROM business b
JOIN socios_cnpj sc ON sc.business_id = b.id
WHERE sc.qualificacao = 'Sócio Administrador'
GROUP BY b.cnpj, b.razao_social
HAVING COUNT(sc.id) > 1
ORDER BY total_socios_mei DESC
LIMIT 20;

-- 6. Verificar empresas que têm sócio MEI mas não deveriam ter (verificar se o CPF está na razão social)
SELECT 
    b.cnpj,
    b.razao_social,
    sc.nome_socio,
    sc.cpf_socio,
    CASE 
        WHEN b.razao_social LIKE '%' || sc.cpf_socio || '%' THEN 'CPF encontrado na razão social'
        ELSE 'CPF não encontrado na razão social'
    END as validacao
FROM business b
JOIN socios_cnpj sc ON sc.business_id = b.id
WHERE sc.qualificacao = 'Sócio Administrador'
LIMIT 50;

-- 7. Verificar distribuição de tamanho de nomes
SELECT 
    CASE 
        WHEN LENGTH(nome_socio) <= 50 THEN '0-50 caracteres'
        WHEN LENGTH(nome_socio) <= 100 THEN '51-100 caracteres'
        WHEN LENGTH(nome_socio) <= 120 THEN '101-120 caracteres'
        ELSE 'Mais de 120 caracteres'
    END as faixa_tamanho,
    COUNT(*) as quantidade,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM socios_cnpj WHERE qualificacao = 'Sócio Administrador'), 2) as percentual
FROM socios_cnpj
WHERE qualificacao = 'Sócio Administrador'
GROUP BY faixa_tamanho
ORDER BY faixa_tamanho;

-- 8. Verificar se há duplicatas (mesmo CPF em empresas diferentes)
SELECT 
    cpf_socio,
    COUNT(DISTINCT business_id) as empresas_diferentes,
    COUNT(*) as total_registros,
    STRING_AGG(DISTINCT cnpj, ', ') as cnpjs
FROM socios_cnpj
WHERE qualificacao = 'Sócio Administrador'
  AND cpf_socio IS NOT NULL
GROUP BY cpf_socio
HAVING COUNT(DISTINCT business_id) > 1
ORDER BY empresas_diferentes DESC
LIMIT 20;

-- 9. Comparar com empresas sem sócios (verificar se o script realmente processou)
SELECT 
    (SELECT COUNT(*) FROM business) as total_empresas,
    (SELECT COUNT(DISTINCT business_id) FROM socios_cnpj WHERE qualificacao = 'Sócio Administrador') as empresas_com_socios_mei,
    (SELECT COUNT(*) FROM business) - (SELECT COUNT(DISTINCT business_id) FROM socios_cnpj WHERE qualificacao = 'Sócio Administrador') as empresas_sem_socios_mei;

-- 10. Verificar registros recentes (últimos inseridos)
SELECT 
    sc.id,
    sc.business_id,
    sc.cnpj,
    sc.nome_socio,
    sc.cpf_socio,
    sc.qualificacao,
    b.razao_social
FROM socios_cnpj sc
JOIN business b ON b.id = sc.business_id
WHERE sc.qualificacao = 'Sócio Administrador'
ORDER BY sc.id DESC
LIMIT 10;

