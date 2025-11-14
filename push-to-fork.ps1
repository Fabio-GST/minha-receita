# Script para fazer push das alterações para o fork
# Execute este script APÓS criar o repositório no GitHub

Write-Host "=== Push para Fork do GitHub ===" -ForegroundColor Cyan
Write-Host ""

# Verificar se o remote fork existe
$forkExists = git remote | Select-String -Pattern "fork"
if (-not $forkExists) {
    Write-Host "Adicionando remote 'fork'..." -ForegroundColor Yellow
    git remote add fork https://github.com/Fabio-GST/minha-receita.git
}

Write-Host "Verificando status do repositório..." -ForegroundColor Yellow
git status --short

Write-Host ""
Write-Host "Fazendo push do branch feature/structured-tables..." -ForegroundColor Yellow
git push -u fork feature/structured-tables

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "=== SUCESSO! ===" -ForegroundColor Green
    Write-Host "Seu fork está disponível em:" -ForegroundColor Green
    Write-Host "https://github.com/Fabio-GST/minha-receita/tree/feature/structured-tables" -ForegroundColor Cyan
} else {
    Write-Host ""
    Write-Host "=== ERRO ===" -ForegroundColor Red
    Write-Host "Certifique-se de que:" -ForegroundColor Yellow
    Write-Host "1. O repositório foi criado em https://github.com/Fabio-GST/minha-receita" -ForegroundColor Yellow
    Write-Host "2. Você tem permissões de escrita no repositório" -ForegroundColor Yellow
    Write-Host "3. Você está autenticado no GitHub (git config --global user.name)" -ForegroundColor Yellow
}

