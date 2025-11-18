package db

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/cuducos/minha-receita/transform"
	"github.com/huandu/go-sqlbuilder"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	companyTableName = "cnpj"
	metaTableName    = "meta"
	cursorFieldName  = "cursor"
	idFieldName      = "id"
	jsonFieldName    = "json"
	keyFieldName     = "key"
	valueFieldName   = "value"
)

//go:embed postgres
var sql embed.FS

type sqlTemplate struct {
	path         fs.DirEntry
	embeddedPath string
	key          string
}

func (s *sqlTemplate) render(p *PostgreSQL) (string, error) {
	t, err := template.ParseFS(sql, s.embeddedPath)
	if err != nil {
		return "", fmt.Errorf("error parsing %s template: %w", s.path, err)
	}
	var b bytes.Buffer
	if err = t.Execute(&b, p); err != nil {
		return "", fmt.Errorf("error rendering %s template: %w", s.path, err)
	}
	return b.String(), nil

}

func newSQLTemplate(f fs.DirEntry) sqlTemplate {
	return sqlTemplate{
		path:         f,
		embeddedPath: "postgres/" + f.Name(),
		key:          strings.TrimSuffix(f.Name(), filepath.Ext(f.Name())),
	}
}

type ExtraIndex struct {
	IsRoot bool
	Name   string
	Value  string
}

func (e *ExtraIndex) NestedPath() string {
	if e.IsRoot {
		slog.Error("cannot not parse nested path for index at the root of the json", "index", e.Value)
		return ""
	}
	p := strings.SplitN(e.Value, ".", 2)
	if len(p) != 2 {
		slog.Error("could not parse nested path", "index", e.Value)
		return ""
	}
	return fmt.Sprintf("$.%s[*].%s", p[0], p[1])
}

// PostgreSQL database interface.
type PostgreSQL struct {
	pool             *pgxpool.Pool
	uri              string
	schema           string
	getCompanyQuery  string
	metaReadQuery    string
	CompanyTableName string
	MetaTableName    string
	CursorFieldName  string
	IDFieldName      string
	JSONFieldName    string
	KeyFieldName     string
	ValueFieldName   string
	ExtraIndexes     []ExtraIndex
}

func (p *PostgreSQL) renderTemplate(key string) (string, error) {
	ls, err := sql.ReadDir("postgres")
	if err != nil {
		return "", fmt.Errorf("error looking for templates: %w", err)
	}
	for _, f := range ls {
		s := newSQLTemplate(f)
		if s.key != key {
			continue
		}
		return s.render(p)
	}
	return "", fmt.Errorf("template %s not found", key)
}

// Close closes the PostgreSQL connection
func (p *PostgreSQL) Close() { p.pool.Close() }

// Pool returns the underlying connection pool for custom queries
func (p *PostgreSQL) Pool() *pgxpool.Pool { return p.pool }

// Schema returns the PostgreSQL schema name
func (p *PostgreSQL) Schema() string { return p.schema }

// CompanyTableFullName is the name of the schame and table in dot-notation.
func (p *PostgreSQL) CompanyTableFullName() string {
	return fmt.Sprintf("%s.%s", p.schema, p.CompanyTableName)
}

// MetaTableFullName is the name of the schame and table in dot-notation.
func (p *PostgreSQL) MetaTableFullName() string {
	return fmt.Sprintf("%s.%s", p.schema, p.MetaTableName)
}

// Create creates the required database table.
func (p *PostgreSQL) Create() error {
	slog.Info("Creating", "table", p.CompanyTableFullName())
	s, err := p.renderTemplate("create")
	if err != nil {
		return fmt.Errorf("error rendering create template: %w", err)
	}
	if _, err := p.pool.Exec(context.Background(), s); err != nil {
		return fmt.Errorf("error creating table with: %s\n%w", s, err)
	}
	return nil
}

// Drop drops the database table created by `Create`.
func (p *PostgreSQL) Drop() error {
	slog.Info("Dropping", "table", p.CompanyTableFullName())
	s, err := p.renderTemplate("drop")
	if err != nil {
		return fmt.Errorf("error rendering drop template: %w", err)
	}
	if _, err := p.pool.Exec(context.Background(), s); err != nil {
		return fmt.Errorf("error dropping table with: %s\n%w", s, err)
	}
	return nil
}

// CreateCompanies performs a copy to create a batch of companies in the
// database. It expects an array and each item should be another array with only
// two items: the ID and the JSON field values.
func (p *PostgreSQL) CreateCompanies(batch [][]string) error {
	b := make([][]any, len(batch))
	for i, r := range batch {
		b[i] = []any{r[0], r[1]}
	}
	_, err := p.pool.CopyFrom(
		context.Background(),
		pgx.Identifier{p.CompanyTableName},
		[]string{idFieldName, jsonFieldName},
		pgx.CopyFromRows(b),
	)
	if err != nil {
		return fmt.Errorf("error while importing data to postgres: %w", err)
	}
	return nil
}

// removeNonDigits removes all non-digit characters from a string
func removeNonDigits(s string) string {
	re := regexp.MustCompile(`\D`)
	return re.ReplaceAllString(s, "")
}

// truncateString truncates a string to the specified maximum length
// If the string is longer, it truncates and adds "..." at the end
func truncateString(s string, maxLen int) string {
	if maxLen <= 0 {
		return s
	}
	if len(s) <= maxLen {
		return s
	}
	// Truncate to maxLen-3 to leave room for "..."
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

// truncateTo120 truncates a string to 120 characters (common VARCHAR(120) limit)
func truncateTo120(s string) string {
	return truncateString(s, 120)
}

// formatPhones concatenates valid phone numbers separated by comma
func formatPhones(company *transform.Company) string {
	var phones []string
	if company.Telefone1 != "" {
		phones = append(phones, company.Telefone1)
	}
	if company.Telefone2 != "" {
		phones = append(phones, company.Telefone2)
	}
	if company.Fax != "" {
		phones = append(phones, company.Fax)
	}
	return strings.Join(phones, ",")
}

// formatSecondaryCNAEs concatenates CNAE codes separated by comma
func formatSecondaryCNAEs(cnaes []transform.CNAE) string {
	if len(cnaes) == 0 {
		return ""
	}
	var codes []string
	for _, cnae := range cnaes {
		codes = append(codes, strconv.Itoa(cnae.Codigo))
	}
	return strings.Join(codes, ",")
}

// formatCNAEPrincipal formats CNAE principal as 7-digit string
func formatCNAEPrincipal(cnae *int) string {
	if cnae == nil {
		return ""
	}
	return fmt.Sprintf("%07d", *cnae)
}

// situacaoCadastralToString converts situacao cadastral code to string
func situacaoCadastralToString(code *int) string {
	if code == nil {
		return ""
	}
	switch *code {
	case 1:
		return "NULA"
	case 2:
		return "ATIVA"
	case 3:
		return "SUSPENSA"
	case 4:
		return "INAPTA"
	case 8:
		return "BAIXADA"
	default:
		return ""
	}
}

// CreateCompaniesStructuredDirect inserts companies directly without JSON conversion
// This is more memory-efficient than CreateCompaniesStructured
// It also handles adding partners even when companies already exist
func (p *PostgreSQL) CreateCompaniesStructuredDirect(batch []transform.Company) error {
	ctx := context.Background()
	
	// Begin transaction with optimized settings for bulk loading
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	
	// Optimize transaction for maximum performance (not worrying about storage)
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = OFF"); err != nil {
		slog.Warn("Could not disable synchronous commit", "error", err)
	}
	// Increase work_mem for better performance (256MB for faster sorting/hashing)
	if _, err := tx.Exec(ctx, "SET LOCAL work_mem = '256MB'"); err != nil {
		slog.Warn("Could not set work_mem", "error", err)
	}
	// Increase maintenance_work_mem for faster index operations
	if _, err := tx.Exec(ctx, "SET LOCAL maintenance_work_mem = '512MB'"); err != nil {
		slog.Warn("Could not set maintenance_work_mem", "error", err)
	}
	// Disable constraint checks temporarily for faster inserts
	if _, err := tx.Exec(ctx, "SET LOCAL constraint_exclusion = off"); err != nil {
		slog.Warn("Could not set constraint_exclusion", "error", err)
	}

	// Prepare batch insert SQL
	businessTable := fmt.Sprintf("%s.business", p.schema)
	insertBusinessSQL := fmt.Sprintf(`INSERT INTO %s (
		cnpj, razao_social, nome_fantasia, situacao_cadastral,
		cnae_principal, tipo_cnae_principal, cnaes_secundarios,
		capital_social, natureza_juridica, qualificacao_responsavel,
		porte_empresa, identificador_matriz_filial,
		data_situacao_cadastral, motivo_situacao_cadastral,
		data_inicio_atividade, email,
		endereco_cep, endereco_numero, endereco_logradouro,
		endereco_bairro, endereco_cidade, endereco_uf,
		endereco_tipo, endereco_complemento, telefones
	) VALUES `, businessTable)

	// Build batch insert values
	values := make([]string, 0, len(batch))
	args := make([]interface{}, 0, len(batch)*25)
	businessIDMap := make(map[string]int64) // Map CNPJ -> business_id for partners
	argIndex := 1

	for _, company := range batch {
		cleanCNPJ := removeNonDigits(company.CNPJ)
		if len(cleanCNPJ) != 14 {
			slog.Warn("invalid CNPJ length", "cnpj", cleanCNPJ)
			continue
		}

		// Prepare business data (same logic as CreateCompaniesStructured)
		var capitalSocial *float64
		if company.CapitalSocial != nil {
			cs := float64(*company.CapitalSocial)
			capitalSocial = &cs
		}

		var naturezaJuridica string
		if company.CodigoNaturezaJuridica != nil {
			naturezaJuridica = strconv.Itoa(*company.CodigoNaturezaJuridica)
		}

		var qualificacaoResponsavel string
		if company.QualificacaoDoResponsavel != nil {
			qualificacaoResponsavel = strconv.Itoa(*company.QualificacaoDoResponsavel)
		}

		var porteEmpresa string
		if company.CodigoPorte != nil {
			porteEmpresa = strconv.Itoa(*company.CodigoPorte)
		}

		identificadorMatrizFilial := "MATRIZ"
		if company.DescricaoMatrizFilial != nil {
			identificadorMatrizFilial = *company.DescricaoMatrizFilial
		} else if company.IdentificadorMatrizFilial != nil && *company.IdentificadorMatrizFilial == 2 {
			identificadorMatrizFilial = "FILIAL"
		}

		var motivoSituacaoCadastral string
		if company.MotivoSituacaoCadastral != nil {
			motivoSituacaoCadastral = strconv.Itoa(*company.MotivoSituacaoCadastral)
		}

		cleanCEP := removeNonDigits(company.CEP)
		if len(cleanCEP) > 8 {
			cleanCEP = cleanCEP[:8]
		}

		var enderecoTipo int16 = 0
		if company.DescricaoTipoDeLogradouro != "" {
			enderecoTipo = 0
		}

		var municipio string
		if company.Municipio != nil {
			municipio = *company.Municipio
		}

		// Remove truncations for performance (let database handle it or use larger limits)
		razaoSocial := company.RazaoSocial
		if len(razaoSocial) > 200 {
			razaoSocial = razaoSocial[:200] // Only truncate if really necessary
		}
		nomeFantasia := company.NomeFantasia
		if len(nomeFantasia) > 200 {
			nomeFantasia = nomeFantasia[:200]
		}
		logradouro := company.Logradouro
		if len(logradouro) > 200 {
			logradouro = logradouro[:200]
		}
		bairro := company.Bairro
		if len(bairro) > 200 {
			bairro = bairro[:200]
		}
		municipioStr := municipio
		if len(municipioStr) > 200 {
			municipioStr = municipioStr[:200]
		}
		complemento := company.Complemento
		if len(complemento) > 200 {
			complemento = complemento[:200]
		}
		
		// Build batch insert values
		values = append(values, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			argIndex, argIndex+1, argIndex+2, argIndex+3, argIndex+4, argIndex+5, argIndex+6, argIndex+7, argIndex+8, argIndex+9,
			argIndex+10, argIndex+11, argIndex+12, argIndex+13, argIndex+14, argIndex+15, argIndex+16, argIndex+17, argIndex+18, argIndex+19,
			argIndex+20, argIndex+21, argIndex+22, argIndex+23, argIndex+24))
		
		args = append(args,
			cleanCNPJ,
			razaoSocial,
			nomeFantasia,
			situacaoCadastralToString(company.SituacaoCadastral),
			formatCNAEPrincipal(company.CNAEFiscal),
			getStringValue(company.CNAEFiscalDescricao),
			formatSecondaryCNAEs(company.CNAESecundarios),
			capitalSocial,
			naturezaJuridica,
			qualificacaoResponsavel,
			porteEmpresa,
			identificadorMatrizFilial,
			convertDate(company.DataSituacaoCadastral),
			motivoSituacaoCadastral,
			convertDate(company.DataInicioAtividade),
			getStringValue(company.Email),
			cleanCEP,
			company.Numero,
			logradouro,
			bairro,
			municipioStr,
			company.UF,
			enderecoTipo,
			complemento,
			formatPhones(&company),
		)
		argIndex += 25
		
		// Store company data for partners batch insert
		businessIDMap[cleanCNPJ] = 0 // Will be filled after batch insert
	}

	// Execute batch insert for all businesses
	if len(values) > 0 {
		insertSQL := insertBusinessSQL + strings.Join(values, ", ") + ` ON CONFLICT (cnpj) DO NOTHING RETURNING id, cnpj`
		rows, err := tx.Query(ctx, insertSQL, args...)
		if err != nil {
			slog.Error("error batch inserting businesses", "error", err, "count", len(values))
			return fmt.Errorf("error batch inserting businesses: %w", err)
		}
		defer rows.Close()
		
		// Track which CNPJs were inserted (returned by RETURNING)
		insertedCNPJs := make(map[string]bool)
		
		// Map CNPJ to business_id for newly inserted businesses
		for rows.Next() {
			var businessID int64
			var cnpj string
			if err := rows.Scan(&businessID, &cnpj); err != nil {
				slog.Warn("error scanning business_id", "error", err)
				continue
			}
			businessIDMap[cnpj] = businessID
			insertedCNPJs[cnpj] = true
		}
		if err := rows.Err(); err != nil {
			slog.Warn("error iterating business rows", "error", err)
		}
		
		// Find business_id for CNPJs that already existed (not returned by RETURNING)
		// Use CNPJs from businessIDMap that weren't inserted
		missingCNPJs := make([]string, 0)
		for cnpj := range businessIDMap {
			if !insertedCNPJs[cnpj] {
				missingCNPJs = append(missingCNPJs, cnpj)
			}
		}
		
		// Batch query to get business_id for existing companies
		if len(missingCNPJs) > 0 {
			placeholders := make([]string, len(missingCNPJs))
			queryArgs := make([]interface{}, len(missingCNPJs))
			for i, cnpj := range missingCNPJs {
				placeholders[i] = fmt.Sprintf("$%d", i+1)
				queryArgs[i] = cnpj
			}
			
			query := fmt.Sprintf("SELECT id, cnpj FROM %s WHERE cnpj IN (%s)", 
				businessTable, strings.Join(placeholders, ","))
			
			existingRows, err := tx.Query(ctx, query, queryArgs...)
			if err != nil {
				slog.Warn("error querying existing businesses", "error", err)
			} else {
				defer existingRows.Close()
				for existingRows.Next() {
					var businessID int64
					var cnpj string
					if err := existingRows.Scan(&businessID, &cnpj); err != nil {
						slog.Warn("error scanning existing business_id", "error", err)
						continue
					}
					businessIDMap[cnpj] = businessID
				}
				if err := existingRows.Err(); err != nil {
					slog.Warn("error iterating existing business rows", "error", err)
				}
			}
		}
	}

	// Batch insert partners for all companies
	sociosCnpjTable := fmt.Sprintf("%s.socios_cnpj", p.schema)
	partnerValues := make([]string, 0)
	partnerArgs := make([]interface{}, 0)
	partnerArgIndex := 1

	for _, company := range batch {
		cleanCNPJ := removeNonDigits(company.CNPJ)
		if len(cleanCNPJ) != 14 {
			continue
		}
		
		businessID, exists := businessIDMap[cleanCNPJ]
		if !exists || businessID == 0 {
			continue // Business not inserted (conflict or error)
		}

		// Insert partners (no DELETE - preserve existing data, skip duplicates)
		for _, partner := range company.QuadroSocietario {
			var dataEntrada *time.Time
			if partner.DataEntradaSociedade != nil {
				dt := time.Time(*partner.DataEntradaSociedade)
				dataEntrada = &dt
			}

			var qualificacao string
			if partner.QualificaoSocio != nil {
				qualificacao = *partner.QualificaoSocio
			}

			cpfSocio := partner.CNPJCPFDoSocio
			// Remove caracteres não numéricos e salva apenas os dígitos encontrados
			// Se tiver mais de 11 dígitos (CNPJ), não insere
			cleanCPFSocio := removeNonDigits(cpfSocio)
			if len(cleanCPFSocio) > 11 {
				slog.Warn("CPF com mais de 11 dígitos, ignorando sócio", "business_id", businessID, "cpf", cleanCPFSocio, "partner", partner.NomeSocio)
				continue // Pula este sócio se tiver mais de 11 dígitos
			}

			var cpfSocioValue interface{}
			if cleanCPFSocio != "" {
				cpfSocioValue = cleanCPFSocio
			} else {
				cpfSocioValue = nil
			}

			// Minimal truncation for performance
			nomeSocio := partner.NomeSocio
			if len(nomeSocio) > 200 {
				nomeSocio = nomeSocio[:200]
			}
			qualificacaoStr := qualificacao
			if len(qualificacaoStr) > 200 {
				qualificacaoStr = qualificacaoStr[:200]
			}

			partnerValues = append(partnerValues, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)",
				partnerArgIndex, partnerArgIndex+1, partnerArgIndex+2, partnerArgIndex+3, partnerArgIndex+4, partnerArgIndex+5))
			partnerArgs = append(partnerArgs, businessID, cleanCNPJ, nomeSocio, cpfSocioValue, dataEntrada, qualificacaoStr)
			partnerArgIndex += 6
		}
	}

	// Execute batch insert for all partners
	if len(partnerValues) > 0 {
		partnerInsertSQL := fmt.Sprintf(`INSERT INTO %s (
			business_id, cnpj, nome_socio, cpf_socio, data_entrada_sociedade, qualificacao
		) VALUES %s ON CONFLICT (cnpj, nome_socio) DO NOTHING`, sociosCnpjTable, strings.Join(partnerValues, ", "))
		
		_, err = tx.Exec(ctx, partnerInsertSQL, partnerArgs...)
		if err != nil {
			slog.Warn("error batch inserting partners", "error", err, "count", len(partnerValues))
			// Continue - partners are optional
		}
	}

	// Commit transaction
	// If there was a rollback and a new transaction started, this commits the new transaction
	if err := tx.Commit(ctx); err != nil {
		// If commit fails, try rollback to clean up
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			slog.Warn("error rolling back failed commit", "error", rbErr)
		}
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}

// CreateCompaniesStructured inserts companies into business and business_partners tables
// This method creates JSON first (for compatibility), but CreateCompaniesStructuredDirect is preferred
// It also handles adding partners even when companies already exist
func (p *PostgreSQL) CreateCompaniesStructured(batch [][]string) error {
	ctx := context.Background()
	
	// Begin transaction with optimized settings for bulk loading
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	
	// Optimize transaction for maximum performance (not worrying about storage)
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = OFF"); err != nil {
		slog.Warn("Could not disable synchronous commit", "error", err)
	}
	// Increase work_mem for better performance
	if _, err := tx.Exec(ctx, "SET LOCAL work_mem = '256MB'"); err != nil {
		slog.Warn("Could not set work_mem", "error", err)
	}
	// Increase maintenance_work_mem for faster index operations
	if _, err := tx.Exec(ctx, "SET LOCAL maintenance_work_mem = '512MB'"); err != nil {
		slog.Warn("Could not set maintenance_work_mem", "error", err)
	}

	// Prepare batch insert SQL
	businessTable := fmt.Sprintf("%s.business", p.schema)
	insertBusinessSQL := fmt.Sprintf(`INSERT INTO %s (
		cnpj, razao_social, nome_fantasia, situacao_cadastral,
		cnae_principal, tipo_cnae_principal, cnaes_secundarios,
		capital_social, natureza_juridica, qualificacao_responsavel,
		porte_empresa, identificador_matriz_filial,
		data_situacao_cadastral, motivo_situacao_cadastral,
		data_inicio_atividade, email,
		endereco_cep, endereco_numero, endereco_logradouro,
		endereco_bairro, endereco_cidade, endereco_uf,
		endereco_tipo, endereco_complemento, telefones
	) VALUES `, businessTable)

	// Build batch insert values
	values := make([]string, 0, len(batch))
	args := make([]interface{}, 0, len(batch)*25)
	businessIDMap := make(map[string]int64) // Map CNPJ -> business_id for partners
	argIndex := 1

	for _, record := range batch {
		if len(record) < 2 {
			slog.Warn("skipping invalid record", "record", record)
			continue
		}

		// Parse JSON
		var company transform.Company
		if err := json.Unmarshal([]byte(record[1]), &company); err != nil {
			slog.Error("error parsing company JSON", "cnpj", record[0], "error", err)
			continue
		}

		// Clean CNPJ
		cleanCNPJ := removeNonDigits(company.CNPJ)
		if len(cleanCNPJ) != 14 {
			slog.Warn("invalid CNPJ length", "cnpj", cleanCNPJ)
			continue
		}

		// Prepare business data
		var capitalSocial *float64
		if company.CapitalSocial != nil {
			cs := float64(*company.CapitalSocial)
			capitalSocial = &cs
		}

		var naturezaJuridica string
		if company.CodigoNaturezaJuridica != nil {
			naturezaJuridica = strconv.Itoa(*company.CodigoNaturezaJuridica)
		}

		var qualificacaoResponsavel string
		if company.QualificacaoDoResponsavel != nil {
			qualificacaoResponsavel = strconv.Itoa(*company.QualificacaoDoResponsavel)
		}

		var porteEmpresa string
		if company.CodigoPorte != nil {
			porteEmpresa = strconv.Itoa(*company.CodigoPorte)
		}

		identificadorMatrizFilial := "MATRIZ"
		if company.DescricaoMatrizFilial != nil {
			identificadorMatrizFilial = *company.DescricaoMatrizFilial
		} else if company.IdentificadorMatrizFilial != nil && *company.IdentificadorMatrizFilial == 2 {
			identificadorMatrizFilial = "FILIAL"
		}

		var motivoSituacaoCadastral string
		if company.MotivoSituacaoCadastral != nil {
			motivoSituacaoCadastral = strconv.Itoa(*company.MotivoSituacaoCadastral)
		}

		cleanCEP := removeNonDigits(company.CEP)
		if len(cleanCEP) > 8 {
			cleanCEP = cleanCEP[:8]
		}

		var enderecoTipo int16 = 0
		// Map logradouro type to numeric (simplified - can be enhanced)
		if company.DescricaoTipoDeLogradouro != "" {
			// Default to 0, can be enhanced with proper mapping
			enderecoTipo = 0
		}

		var municipio string
		if company.Municipio != nil {
			municipio = *company.Municipio
		}

		// Minimal truncation for performance
		razaoSocial := company.RazaoSocial
		if len(razaoSocial) > 200 {
			razaoSocial = razaoSocial[:200]
		}
		nomeFantasia := company.NomeFantasia
		if len(nomeFantasia) > 200 {
			nomeFantasia = nomeFantasia[:200]
		}
		logradouro := company.Logradouro
		if len(logradouro) > 200 {
			logradouro = logradouro[:200]
		}
		bairro := company.Bairro
		if len(bairro) > 200 {
			bairro = bairro[:200]
		}
		municipioStr := municipio
		if len(municipioStr) > 200 {
			municipioStr = municipioStr[:200]
		}
		complemento := company.Complemento
		if len(complemento) > 200 {
			complemento = complemento[:200]
		}
		
		// Build batch insert values
		values = append(values, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			argIndex, argIndex+1, argIndex+2, argIndex+3, argIndex+4, argIndex+5, argIndex+6, argIndex+7, argIndex+8, argIndex+9,
			argIndex+10, argIndex+11, argIndex+12, argIndex+13, argIndex+14, argIndex+15, argIndex+16, argIndex+17, argIndex+18, argIndex+19,
			argIndex+20, argIndex+21, argIndex+22, argIndex+23, argIndex+24))
		
		args = append(args,
			cleanCNPJ,
			razaoSocial,
			nomeFantasia,
			situacaoCadastralToString(company.SituacaoCadastral),
			formatCNAEPrincipal(company.CNAEFiscal),
			getStringValue(company.CNAEFiscalDescricao),
			formatSecondaryCNAEs(company.CNAESecundarios),
			capitalSocial,
			naturezaJuridica,
			qualificacaoResponsavel,
			porteEmpresa,
			identificadorMatrizFilial,
			convertDate(company.DataSituacaoCadastral),
			motivoSituacaoCadastral,
			convertDate(company.DataInicioAtividade),
			getStringValue(company.Email),
			cleanCEP,
			company.Numero,
			logradouro,
			bairro,
			municipioStr,
			company.UF,
			enderecoTipo,
			complemento,
			formatPhones(&company),
		)
		argIndex += 25
		
		// Store company data for partners batch insert
		businessIDMap[cleanCNPJ] = 0 // Will be filled after batch insert
	}

	// Execute batch insert for all businesses
	if len(values) > 0 {
		insertSQL := insertBusinessSQL + strings.Join(values, ", ") + ` ON CONFLICT (cnpj) DO NOTHING RETURNING id, cnpj`
		rows, err := tx.Query(ctx, insertSQL, args...)
		if err != nil {
			slog.Error("error batch inserting businesses", "error", err, "count", len(values))
			return fmt.Errorf("error batch inserting businesses: %w", err)
		}
		defer rows.Close()
		
		// Track which CNPJs were inserted (returned by RETURNING)
		insertedCNPJs := make(map[string]bool)
		
		// Map CNPJ to business_id for newly inserted businesses
		for rows.Next() {
			var businessID int64
			var cnpj string
			if err := rows.Scan(&businessID, &cnpj); err != nil {
				slog.Warn("error scanning business_id", "error", err)
				continue
			}
			businessIDMap[cnpj] = businessID
			insertedCNPJs[cnpj] = true
		}
		if err := rows.Err(); err != nil {
			slog.Warn("error iterating business rows", "error", err)
		}
		
		// Find business_id for CNPJs that already existed (not returned by RETURNING)
		// Use CNPJs from businessIDMap that weren't inserted
		missingCNPJs := make([]string, 0)
		for cnpj := range businessIDMap {
			if !insertedCNPJs[cnpj] {
				missingCNPJs = append(missingCNPJs, cnpj)
			}
		}
		
		// Batch query to get business_id for existing companies
		if len(missingCNPJs) > 0 {
			placeholders := make([]string, len(missingCNPJs))
			queryArgs := make([]interface{}, len(missingCNPJs))
			for i, cnpj := range missingCNPJs {
				placeholders[i] = fmt.Sprintf("$%d", i+1)
				queryArgs[i] = cnpj
			}
			
			query := fmt.Sprintf("SELECT id, cnpj FROM %s WHERE cnpj IN (%s)", 
				businessTable, strings.Join(placeholders, ","))
			
			existingRows, err := tx.Query(ctx, query, queryArgs...)
			if err != nil {
				slog.Warn("error querying existing businesses", "error", err)
			} else {
				defer existingRows.Close()
				for existingRows.Next() {
					var businessID int64
					var cnpj string
					if err := existingRows.Scan(&businessID, &cnpj); err != nil {
						slog.Warn("error scanning existing business_id", "error", err)
						continue
					}
					businessIDMap[cnpj] = businessID
				}
				if err := existingRows.Err(); err != nil {
					slog.Warn("error iterating existing business rows", "error", err)
				}
			}
		}
	}

	// Batch insert partners for all companies
	sociosCnpjTable := fmt.Sprintf("%s.socios_cnpj", p.schema)
	partnerValues := make([]string, 0)
	partnerArgs := make([]interface{}, 0)
	partnerArgIndex := 1

	for _, record := range batch {
		if len(record) < 2 {
			slog.Warn("skipping invalid record", "record", record)
			continue
		}

		// Parse JSON
		var company transform.Company
		if err := json.Unmarshal([]byte(record[1]), &company); err != nil {
			slog.Error("error parsing company JSON", "cnpj", record[0], "error", err)
			continue
		}

		cleanCNPJ := removeNonDigits(company.CNPJ)
		if len(cleanCNPJ) != 14 {
			slog.Warn("invalid CNPJ length", "cnpj", cleanCNPJ)
			continue
		}
		
		businessID, exists := businessIDMap[cleanCNPJ]
		if !exists || businessID == 0 {
			continue // Business not inserted (conflict or error)
		}

		// Insert partners (no DELETE - preserve existing data, skip duplicates)
		for _, partner := range company.QuadroSocietario {
			var dataEntrada *time.Time
			if partner.DataEntradaSociedade != nil {
				dt := time.Time(*partner.DataEntradaSociedade)
				dataEntrada = &dt
			}

			var qualificacao string
			if partner.QualificaoSocio != nil {
				qualificacao = *partner.QualificaoSocio
			}

			// Extract CPF from partner (may be masked like ***220050**)
			cpfSocio := partner.CNPJCPFDoSocio
			// Remove caracteres não numéricos e salva apenas os dígitos encontrados
			// Se tiver mais de 11 dígitos (CNPJ), não insere
			cleanCPFSocio := removeNonDigits(cpfSocio)
			if len(cleanCPFSocio) > 11 {
				slog.Warn("CPF com mais de 11 dígitos, ignorando sócio", "business_id", businessID, "cpf", cleanCPFSocio, "partner", partner.NomeSocio)
				continue // Pula este sócio se tiver mais de 11 dígitos
			}

			// Insert partner into socios_cnpj
			var cpfSocioValue interface{}
			if cleanCPFSocio != "" {
				cpfSocioValue = cleanCPFSocio
			} else {
				cpfSocioValue = nil
			}

			// Minimal truncation for performance
			nomeSocio := partner.NomeSocio
			if len(nomeSocio) > 200 {
				nomeSocio = nomeSocio[:200]
			}
			qualificacaoStr := qualificacao
			if len(qualificacaoStr) > 200 {
				qualificacaoStr = qualificacaoStr[:200]
			}

			partnerValues = append(partnerValues, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)",
				partnerArgIndex, partnerArgIndex+1, partnerArgIndex+2, partnerArgIndex+3, partnerArgIndex+4, partnerArgIndex+5))
			partnerArgs = append(partnerArgs, businessID, cleanCNPJ, nomeSocio, cpfSocioValue, dataEntrada, qualificacaoStr)
			partnerArgIndex += 6
		}
	}

	// Execute batch insert for all partners
	if len(partnerValues) > 0 {
		partnerInsertSQL := fmt.Sprintf(`INSERT INTO %s (
			business_id, cnpj, nome_socio, cpf_socio, data_entrada_sociedade, qualificacao
		) VALUES %s ON CONFLICT (cnpj, nome_socio) DO NOTHING`, sociosCnpjTable, strings.Join(partnerValues, ", "))
		
		_, err = tx.Exec(ctx, partnerInsertSQL, partnerArgs...)
		if err != nil {
			slog.Warn("error batch inserting partners", "error", err, "count", len(partnerValues))
			// Continue - partners are optional
		}
	}

	// Commit transaction
	// If there was a rollback and a new transaction started, this commits the new transaction
	if err := tx.Commit(ctx); err != nil {
		// If commit fails, try rollback to clean up
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			slog.Warn("error rolling back failed commit", "error", rbErr)
		}
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}

// Helper functions
func getStringValue(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func convertDate(d interface{}) *time.Time {
	if d == nil {
		return nil
	}
	// date is type date time.Time (an alias), so we can convert it
	// Use reflection to get the underlying time.Time value
	v := reflect.ValueOf(d)
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return nil
		}
		v = v.Elem()
	}
	// date is an alias for time.Time, so convert to time.Time
	// Since date is not exported, we use reflection to access the underlying value
	if v.CanInterface() {
		// date is essentially time.Time, so we can convert it
		if v.CanConvert(reflect.TypeOf(time.Time{})) {
			t := v.Convert(reflect.TypeOf(time.Time{})).Interface().(time.Time)
			return &t
		}
		// Try direct conversion if it's already time.Time
		if t, ok := v.Interface().(time.Time); ok {
			return &t
		}
	}
	return nil
}

// GetCompany returns the JSON of a company based on a CNPJ number.
func (p *PostgreSQL) GetCompany(id string) (string, error) {
	ctx := context.Background()
	rows, err := p.pool.Query(ctx, p.getCompanyQuery, id)
	if err != nil {
		return "", fmt.Errorf("error looking for cnpj %s: %w", id, err)
	}
	j, err := pgx.CollectOneRow(rows, pgx.RowTo[string])
	if err != nil {
		return "", fmt.Errorf("error reading cnpj %s: %w", id, err)
	}
	return j, nil
}

func (p *PostgreSQL) searchQuery(q *Query) *sqlbuilder.SelectBuilder {
	b := sqlbuilder.PostgreSQL.NewSelectBuilder()
	b.Select(p.CursorFieldName, p.JSONFieldName)
	b.From(p.CompanyTableFullName())
	b.OrderByAsc(p.CursorFieldName)
	b.Limit(int(q.Limit))
	if q.Cursor != nil {
		c, err := q.CursorAsInt()
		if err == nil {
			b.Where(b.GreaterThan(p.CursorFieldName, c))
		}
	}
	if len(q.UF) > 0 {
		c := make([]string, len(q.UF))
		for i, v := range q.UF {
			c[i] = fmt.Sprintf(`json -> 'uf' = '"%s"'::jsonb`, v)
		}
		b.Where(b.Or(c...))
	}
	if len(q.Municipio) > 0 {
		c := make([]string, len(q.Municipio)*2)
		for i, v := range q.Municipio {
			c[i] = fmt.Sprintf("json -> 'codigo_municipio' = '%d'::jsonb", v)
			c[i+len(q.Municipio)] = fmt.Sprintf("json -> 'codigo_municipio_ibge' = '%d'::jsonb", v)
		}
		b.Where(b.Or(c...))
	}
	if len(q.NaturezaJuridica) > 0 {
		c := make([]string, len(q.NaturezaJuridica))
		for i, v := range q.NaturezaJuridica {
			c[i] = fmt.Sprintf("json -> 'codigo_natureza_juridica' = '%d'::jsonb", v)
		}
		b.Where(b.Or(c...))
	}
	if len(q.CNAEFiscal) > 0 {
		c := make([]string, len(q.CNAEFiscal))
		for i, v := range q.CNAEFiscal {
			c[i] = fmt.Sprintf("json -> 'cnae_fiscal' = '%d'::jsonb", v)
		}
		b.Where(b.Or(c...))
	}
	if len(q.CNAE) > 0 {
		c := make([]string, len(q.CNAE)+1)
		s := make([]string, len(q.CNAE))
		for i, v := range q.CNAE {
			s[i] = fmt.Sprintf("%d", v)
			c[i] = fmt.Sprintf("json -> 'cnae_fiscal' = '%d'::jsonb", v)
		}
		c[len(q.CNAE)] = fmt.Sprintf(
			"jsonb_path_query_array(json, '$.cnaes_secundarios[*].codigo') @> '[%s]'",
			strings.Join(s, ","),
		)
		b.Where(b.Or(c...))
	}
	if len(q.CNPF) > 0 {
		c := make([]string, len(q.CNPF))
		for i, v := range q.CNPF {
			c[i] = fmt.Sprintf(`jsonb_path_query_array(json, '$.qsa[*].cnpj_cpf_do_socio') @> '["%s"]'`, v)
		}
		b.Where(b.Or(c...))
	}
	return b
}

type postgresRecord struct {
	Cursor  int
	Company string
}

// Search returns paginated results with JSON for companies bases on a search
// query
func (p *PostgreSQL) Search(ctx context.Context, q *Query) (string, error) {
	s, a := p.searchQuery(q).Build()
	slog.Debug("paginated search", "query", s, "args", a)
	rows, err := p.pool.Query(ctx, s, a...)
	if err != nil {
		return "", fmt.Errorf("error searching for %#v: %w", q, err)
	}
	rs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[postgresRecord])
	if err != nil {
		return "", fmt.Errorf("error reading search result for %#v: %w", q, err)
	}
	var cs []string
	for _, r := range rs {
		cs = append(cs, r.Company)
	}
	var cur string
	if len(rs) == int(q.Limit) {
		cur = fmt.Sprintf("%d", rs[len(rs)-1].Cursor)
	}
	return newPage(cs, cur), nil

}

// PreLoad runs before starting to load data into the database. Currently it
// disables autovacuum on PostgreSQL and optimizes storage.
func (p *PostgreSQL) PreLoad() error {
	// Optimize storage before loading: run VACUUM to reclaim space
	// Use VACUUM (not VACUUM FULL) to avoid blocking and reduce disk usage
	// Run VACUUM ANALYZE on specific tables to reclaim space more efficiently
	slog.Info("Running VACUUM to optimize storage before loading...")
	
	// Check which tables exist and vacuum them specifically
	checkQuery := `SELECT EXISTS (
		SELECT FROM information_schema.tables 
		WHERE table_schema = $1 AND table_name = 'business'
	)`
	var businessExists bool
	err := p.pool.QueryRow(context.Background(), checkQuery, p.schema).Scan(&businessExists)
	if err == nil && businessExists {
		// Create functional index for CNPJ base lookups (optimizes partner imports)
		businessTable := fmt.Sprintf("%s.business", p.schema)
		createIndexSQL := fmt.Sprintf(`
			CREATE INDEX IF NOT EXISTS idx_business_cnpj_base 
			ON %s (LEFT(cnpj, 8))
		`, businessTable)
		if _, err := p.pool.Exec(context.Background(), createIndexSQL); err != nil {
			slog.Warn("Could not create functional index for CNPJ base lookups", "error", err)
			// Continue anyway - the query will still work, just slower
		} else {
			slog.Info("Created functional index for CNPJ base lookups (optimizes partner imports)")
		}
		
		// Vacuum business and socios_cnpj tables specifically
		vacuumSQL := fmt.Sprintf("VACUUM ANALYZE %s.business, %s.socios_cnpj", p.schema, p.schema)
		if _, err := p.pool.Exec(context.Background(), vacuumSQL); err != nil {
			slog.Warn("Could not run VACUUM on business tables before loading", "error", err)
			// Try generic VACUUM as fallback
			if _, err := p.pool.Exec(context.Background(), "VACUUM"); err != nil {
				slog.Warn("Could not run generic VACUUM before loading", "error", err)
			}
		} else {
			slog.Info("VACUUM completed successfully on business tables")
		}
	} else {
		// Try generic VACUUM for JSON mode
		vacuumSQL := "VACUUM ANALYZE"
		if _, err := p.pool.Exec(context.Background(), vacuumSQL); err != nil {
			slog.Warn("Could not run VACUUM before loading", "error", err)
			// Continue anyway, not critical - VACUUM can fail if disk is full
		} else {
			slog.Info("VACUUM completed successfully")
		}
	}
	
	// Disable autovacuum during bulk loading to save space and improve performance
	slog.Info("Disabling autovacuum for bulk loading...")
	disableAutovacuumSQL := fmt.Sprintf(`
		ALTER TABLE %s.business SET (autovacuum_enabled = false);
		ALTER TABLE %s.socios_cnpj SET (autovacuum_enabled = false);
	`, p.schema, p.schema)
	if _, err := p.pool.Exec(context.Background(), disableAutovacuumSQL); err != nil {
		slog.Warn("Could not disable autovacuum", "error", err)
		// Continue anyway
	}
	
	// Use the businessExists variable already checked above
	businessTable := fmt.Sprintf("%s.business", p.schema)
	cnpjTable := p.CompanyTableFullName()
	
	if businessExists {
		// In structured mode, we need to set UNLOGGED on all related tables
		// Order matters: tables referenced by business must be UNLOGGED first
		// The error shows business references socios_cnpj, so socios_cnpj must be first
		// Order: 1. socios_cnpj, 2. business_partners, 3. business
		
		// Step 1: Set socios_cnpj to UNLOGGED first (referenced by business)
		checkQuery = `SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = $1 AND table_name = 'socios_cnpj'
		)`
		var sociosCnpjExists bool
		err = p.pool.QueryRow(context.Background(), checkQuery, p.schema).Scan(&sociosCnpjExists)
		if err == nil && sociosCnpjExists {
			sociosCnpjTable := fmt.Sprintf("%s.socios_cnpj", p.schema)
			sql := fmt.Sprintf("ALTER TABLE %s SET UNLOGGED", sociosCnpjTable)
			if _, err := p.pool.Exec(context.Background(), sql); err != nil {
				slog.Warn("Could not set socios_cnpj to UNLOGGED", "error", err)
			} else {
				slog.Debug("Set socios_cnpj table to UNLOGGED")
			}
		}
		
		// Step 2: Set business_partners to UNLOGGED (if it exists and is referenced by business)
		checkQuery = `SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = $1 AND table_name = 'business_partners'
		)`
		var businessPartnersExists bool
		err = p.pool.QueryRow(context.Background(), checkQuery, p.schema).Scan(&businessPartnersExists)
		if err == nil && businessPartnersExists {
			businessPartnersTable := fmt.Sprintf("%s.business_partners", p.schema)
			sql := fmt.Sprintf("ALTER TABLE %s SET UNLOGGED", businessPartnersTable)
			if _, err := p.pool.Exec(context.Background(), sql); err != nil {
				slog.Warn("Could not set business_partners to UNLOGGED", "error", err)
			} else {
				slog.Debug("Set business_partners table to UNLOGGED")
			}
		}
		
		// Step 3: Set business to UNLOGGED (after all tables it references)
		// If this fails due to disk space, continue anyway - UNLOGGED is an optimization, not required
		sql := fmt.Sprintf("ALTER TABLE %s SET UNLOGGED", businessTable)
		if _, err := p.pool.Exec(context.Background(), sql); err != nil {
			slog.Warn("Could not set business to UNLOGGED (continuing anyway)", "table", businessTable, "error", err)
			slog.Info("Continuing with LOGGED mode - this will use more disk space but will work")
		} else {
			slog.Debug("Set business table to UNLOGGED", "table", businessTable)
		}
		
		return nil
	}
	
	// Check if cnpj table exists (JSON mode)
	checkQuery = `SELECT EXISTS (
		SELECT FROM information_schema.tables 
		WHERE table_schema = $1 AND table_name = $2
	)`
	var cnpjExists bool
	err = p.pool.QueryRow(context.Background(), checkQuery, p.schema, p.CompanyTableName).Scan(&cnpjExists)
	if err != nil {
		return fmt.Errorf("error checking if cnpj table exists: %w", err)
	}
	
	if cnpjExists {
		// Verify the table exists before trying to alter it
		verifyQuery := `SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = $1 AND table_name = $2
		)`
		var tableExists bool
		err = p.pool.QueryRow(context.Background(), verifyQuery, p.schema, p.CompanyTableName).Scan(&tableExists)
		if err != nil {
			return fmt.Errorf("error verifying table exists: %w", err)
		}
		
		if !tableExists {
			slog.Warn("Table does not exist yet, skipping UNLOGGED optimization", "table", cnpjTable)
			return nil
		}
		
		// Apply UNLOGGED to the table
		// If this fails due to disk space, continue anyway - UNLOGGED is an optimization, not required
		sql := fmt.Sprintf("ALTER TABLE %s SET UNLOGGED", cnpjTable)
		if _, err := p.pool.Exec(context.Background(), sql); err != nil {
			slog.Warn("Could not set cnpj table to UNLOGGED (continuing anyway)", "table", cnpjTable, "error", err)
			slog.Info("Continuing with LOGGED mode - this will use more disk space but will work")
		} else {
			slog.Debug("Set cnpj table to UNLOGGED", "table", cnpjTable)
		}
		return nil
	}
	
	// If neither exists, skip optimization
	slog.Warn("No tables found, skipping UNLOGGED optimization")
	return nil
}

// PostLoad runs after loading data into the database. Currently it re-enables
// autovacuum on PostgreSQL and optimizes storage.
func (p *PostgreSQL) PostLoad() error {
	// Check which table exists: business (structured) or cnpj (JSON)
	// Try business table first (structured mode)
	businessTable := fmt.Sprintf("%s.business", p.schema)
	cnpjTable := p.CompanyTableFullName()
	
	// Check if business table exists
	checkQuery := `SELECT EXISTS (
		SELECT FROM information_schema.tables 
		WHERE table_schema = $1 AND table_name = 'business'
	)`
	var businessExists bool
	err := p.pool.QueryRow(context.Background(), checkQuery, p.schema).Scan(&businessExists)
	if err != nil {
		return fmt.Errorf("error checking if business table exists: %w", err)
	}
	
	// Re-enable autovacuum after loading
	slog.Info("Re-enabling autovacuum after loading...")
	enableAutovacuumSQL := fmt.Sprintf(`
		ALTER TABLE %s.business SET (autovacuum_enabled = true);
		ALTER TABLE %s.socios_cnpj SET (autovacuum_enabled = true);
	`, p.schema, p.schema)
	if _, err := p.pool.Exec(context.Background(), enableAutovacuumSQL); err != nil {
		slog.Warn("Could not re-enable autovacuum", "error", err)
		// Continue anyway
	}
	
	if businessExists {
		// In structured mode, set LOGGED in reverse order of UNLOGGED
		// Order: 1. business first, 2. business_partners, 3. socios_cnpj
		
		// Step 1: Set business to LOGGED first
		sql := fmt.Sprintf("ALTER TABLE %s SET LOGGED", businessTable)
		if _, err := p.pool.Exec(context.Background(), sql); err != nil {
			return fmt.Errorf("error during post load: %s\n%w", sql, err)
		}
		slog.Debug("Set business table to LOGGED", "table", businessTable)
		
		// Step 2: Set business_partners to LOGGED
		checkQuery = `SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = $1 AND table_name = 'business_partners'
		)`
		var businessPartnersExists bool
		err = p.pool.QueryRow(context.Background(), checkQuery, p.schema).Scan(&businessPartnersExists)
		if err == nil && businessPartnersExists {
			businessPartnersTable := fmt.Sprintf("%s.business_partners", p.schema)
			sql := fmt.Sprintf("ALTER TABLE %s SET LOGGED", businessPartnersTable)
			if _, err := p.pool.Exec(context.Background(), sql); err != nil {
				slog.Warn("Could not set business_partners to LOGGED", "error", err)
			} else {
				slog.Debug("Set business_partners table to LOGGED")
			}
		}
		
		// Step 3: Set socios_cnpj to LOGGED last
		checkQuery = `SELECT EXISTS (
			SELECT FROM information_schema.tables 
			WHERE table_schema = $1 AND table_name = 'socios_cnpj'
		)`
		var sociosCnpjExists bool
		err = p.pool.QueryRow(context.Background(), checkQuery, p.schema).Scan(&sociosCnpjExists)
		if err == nil && sociosCnpjExists {
			sociosCnpjTable := fmt.Sprintf("%s.socios_cnpj", p.schema)
			sql := fmt.Sprintf("ALTER TABLE %s SET LOGGED", sociosCnpjTable)
			if _, err := p.pool.Exec(context.Background(), sql); err != nil {
				slog.Warn("Could not set socios_cnpj to LOGGED", "error", err)
			} else {
				slog.Debug("Set socios_cnpj table to LOGGED")
			}
		}
		
		// Run VACUUM to reclaim space after loading
		// Use VACUUM (not VACUUM FULL) to avoid blocking and reduce disk usage
		slog.Info("Running VACUUM to optimize storage after loading...")
		vacuumSQL := fmt.Sprintf("VACUUM %s.business, %s.socios_cnpj", p.schema, p.schema)
		if _, err := p.pool.Exec(context.Background(), vacuumSQL); err != nil {
			slog.Warn("Could not run VACUUM after loading", "error", err)
			// Continue anyway - VACUUM can fail if disk is full
		} else {
			slog.Info("VACUUM completed successfully")
			// Run ANALYZE separately to update statistics
			analyzeSQL := fmt.Sprintf("ANALYZE %s.business, %s.socios_cnpj", p.schema, p.schema)
			if _, err := p.pool.Exec(context.Background(), analyzeSQL); err != nil {
				slog.Warn("Could not run ANALYZE", "error", err)
			}
		}
		
		return nil
	}
	
	// Check if cnpj table exists (JSON mode)
	checkQuery = `SELECT EXISTS (
		SELECT FROM information_schema.tables 
		WHERE table_schema = $1 AND table_name = $2
	)`
	var cnpjExists bool
	err = p.pool.QueryRow(context.Background(), checkQuery, p.schema, p.CompanyTableName).Scan(&cnpjExists)
	if err != nil {
		return fmt.Errorf("error checking if cnpj table exists: %w", err)
	}
	
	if cnpjExists {
		// Apply LOGGED to the table
		sql := fmt.Sprintf("ALTER TABLE %s SET LOGGED", cnpjTable)
		if _, err := p.pool.Exec(context.Background(), sql); err != nil {
			return fmt.Errorf("error during post load: %s\n%w", sql, err)
		}
		
		// Run VACUUM to reclaim space after loading
		// Use VACUUM (not VACUUM FULL) to avoid blocking and reduce disk usage
		slog.Info("Running VACUUM to optimize storage after loading...")
		vacuumSQL := fmt.Sprintf("VACUUM %s", cnpjTable)
		if _, err := p.pool.Exec(context.Background(), vacuumSQL); err != nil {
			slog.Warn("Could not run VACUUM after loading", "error", err)
			// Continue anyway - VACUUM can fail if disk is full
		} else {
			slog.Info("VACUUM completed successfully")
			// Run ANALYZE separately to update statistics
			analyzeSQL := fmt.Sprintf("ANALYZE %s", cnpjTable)
			if _, err := p.pool.Exec(context.Background(), analyzeSQL); err != nil {
				slog.Warn("Could not run ANALYZE", "error", err)
			}
		}
		
		return nil
	}
	
	// If neither exists, skip optimization
	slog.Warn("No tables found, skipping LOGGED optimization")
	return nil
}

// ImportPartnersOnly imports only partners data from Socios CSV files into socios_cnpj table
// This function reads partner data and inserts it directly into socios_cnpj table
// It requires the business table to exist and will get business_id from it based on CNPJ base (8 digits)
// The cnpj parameter can be either a full CNPJ (14 digits) or a CNPJ base (8 digits)
func (p *PostgreSQL) ImportPartnersOnly(partners []transform.PartnerData, cnpj string) error {
	ctx := context.Background()
	
	// Clean CNPJ
	cleanCNPJ := removeNonDigits(cnpj)
	
	// CNPJ base has 8 digits, full CNPJ has 14 digits
	// If we have 8 digits, it's a base and we need to find all businesses with that base
	// If we have 14 digits, it's a full CNPJ
	var businessIDs []int
	businessTable := fmt.Sprintf("%s.business", p.schema)
	
	if len(cleanCNPJ) == 8 {
		// CNPJ base: find all businesses that start with this base
		// Using LEFT() function which is optimized and can use functional index
		// This is more performatic than LIKE when we have an index on LEFT(cnpj, 8)
		query := fmt.Sprintf(
			"SELECT id FROM %s WHERE cnpj LIKE $1 || '%%'",
			businessTable,
	)
	rows, err := p.pool.Query(ctx, query, cleanCNPJ)
	
		if err != nil {
			return fmt.Errorf("error querying businesses for CNPJ base %s: %w", cleanCNPJ, err)
		}
		defer rows.Close()
		
		for rows.Next() {
			var businessID int
			if err := rows.Scan(&businessID); err != nil {
				return fmt.Errorf("error scanning business_id: %w", err)
			}
			businessIDs = append(businessIDs, businessID)
		}
		
		if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating business rows: %w", err)
		}
		
		if len(businessIDs) == 0 {
			slog.Warn("No businesses found for CNPJ base, skipping partners", "cnpj_base", cleanCNPJ)
			return nil // Skip if no businesses found
		}
	} else if len(cleanCNPJ) == 14 {
		// Full CNPJ: find the specific business
		var businessID int
		err := p.pool.QueryRow(ctx, fmt.Sprintf("SELECT id FROM %s WHERE cnpj = $1", businessTable), cleanCNPJ).Scan(&businessID)
		if err != nil {
			if err == pgx.ErrNoRows {
				slog.Warn("Business not found for CNPJ, skipping partners", "cnpj", cleanCNPJ)
				return nil // Skip if business doesn't exist
			}
			return fmt.Errorf("error getting business_id for CNPJ %s: %w", cleanCNPJ, err)
		}
		businessIDs = []int{businessID}
	} else {
		return fmt.Errorf("invalid CNPJ length (expected 8 or 14 digits): %s (length: %d)", cleanCNPJ, len(cleanCNPJ))
	}
	
	// Process partners for each business found using batch inserts for better performance
	sociosCnpjTable := fmt.Sprintf("%s.socios_cnpj", p.schema)
	
	for _, businessID := range businessIDs {
		// Get the full CNPJ for this business to use in socios_cnpj table
		var fullCNPJ string
		err := p.pool.QueryRow(ctx, fmt.Sprintf("SELECT cnpj FROM %s WHERE id = $1", businessTable), businessID).Scan(&fullCNPJ)
		if err != nil {
			slog.Warn("error getting CNPJ for business_id", "business_id", businessID, "error", err)
			continue
		}
		
		// Begin transaction
		tx, err := p.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("error beginning transaction: %w", err)
		}
		
		// Optimize transaction for bulk loading
		if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = OFF"); err != nil {
			slog.Warn("Could not disable synchronous commit", "error", err)
		}
		if _, err := tx.Exec(ctx, "SET LOCAL work_mem = '64MB'"); err != nil {
			slog.Warn("Could not set work_mem", "error", err)
		}
		
		// Use batch insert for better performance (no DELETE - preserve existing data)
		if len(partners) > 0 {
			// Prepare batch insert statement
			insertSQL := fmt.Sprintf(`INSERT INTO %s (
				business_id, cnpj, nome_socio, cpf_socio, data_entrada_sociedade, qualificacao
			) VALUES `, sociosCnpjTable)
			
			// Build values and args for batch insert
			values := make([]string, 0, len(partners))
			args := make([]interface{}, 0, len(partners)*6)
			argIndex := 1
			
			for _, partner := range partners {
				var dataEntrada *time.Time
				if partner.DataEntradaSociedade != nil {
					dt := time.Time(*partner.DataEntradaSociedade)
					dataEntrada = &dt
				}
				
				var qualificacao string
				if partner.QualificaoSocio != nil {
					qualificacao = *partner.QualificaoSocio
				}
				
				// Extract CPF from partner (may be masked like ***220050**)
				cpfSocio := partner.CNPJCPFDoSocio
				cleanCPFSocio := removeNonDigits(cpfSocio)
				// Se tiver mais de 11 dígitos (CNPJ), não insere
				if len(cleanCPFSocio) > 11 {
					slog.Warn("CPF com mais de 11 dígitos, ignorando sócio", "business_id", businessID, "cpf", cleanCPFSocio, "partner", partner.NomeSocio)
					continue // Pula este sócio se tiver mais de 11 dígitos
				}
				
				var cpfSocioValue interface{}
				if cleanCPFSocio != "" {
					cpfSocioValue = cleanCPFSocio
				} else {
					cpfSocioValue = nil
				}
				
				// Truncate partner fields that may exceed database limits
				nomeSocio := truncateTo120(partner.NomeSocio)
				qualificacaoTrunc := truncateTo120(qualificacao)
				
				values = append(values, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)", 
					argIndex, argIndex+1, argIndex+2, argIndex+3, argIndex+4, argIndex+5))
				args = append(args, businessID, fullCNPJ, nomeSocio, cpfSocioValue, dataEntrada, qualificacaoTrunc)
				argIndex += 6
			}
			
			// Execute batch insert with ON CONFLICT to skip duplicates
			if len(values) > 0 {
				insertSQL += strings.Join(values, ", ")
				insertSQL += fmt.Sprintf(` ON CONFLICT (cnpj, nome_socio) DO NOTHING`)
				_, err = tx.Exec(ctx, insertSQL, args...)
				if err != nil {
					slog.Warn("error batch inserting partners", "business_id", businessID, "partners_count", len(partners), "error", err)
					// Fallback to individual inserts if batch fails
					for _, partner := range partners {
						var dataEntrada *time.Time
						if partner.DataEntradaSociedade != nil {
							dt := time.Time(*partner.DataEntradaSociedade)
							dataEntrada = &dt
						}
						
						var qualificacao string
						if partner.QualificaoSocio != nil {
							qualificacao = *partner.QualificaoSocio
						}
						
						cpfSocio := partner.CNPJCPFDoSocio
						cleanCPFSocio := removeNonDigits(cpfSocio)
						// Se tiver mais de 11 dígitos (CNPJ), não insere
						if len(cleanCPFSocio) > 11 {
							slog.Warn("CPF com mais de 11 dígitos, ignorando sócio (fallback)", "business_id", businessID, "cpf", cleanCPFSocio, "partner", partner.NomeSocio)
							continue // Pula este sócio se tiver mais de 11 dígitos
						}
						
						var cpfSocioValue interface{}
						if cleanCPFSocio != "" {
							cpfSocioValue = cleanCPFSocio
						} else {
							cpfSocioValue = nil
						}
						
						nomeSocio := truncateTo120(partner.NomeSocio)
						qualificacaoTrunc := truncateTo120(qualificacao)
						
						_, err = tx.Exec(ctx, fmt.Sprintf(`INSERT INTO %s (
							business_id, cnpj, nome_socio, cpf_socio, data_entrada_sociedade, qualificacao
						) VALUES ($1, $2, $3, $4, $5, $6)
						ON CONFLICT (cnpj, nome_socio) DO NOTHING`, sociosCnpjTable),
							businessID, fullCNPJ, nomeSocio, cpfSocioValue, dataEntrada, qualificacaoTrunc,
						)
						if err != nil {
							slog.Warn("error inserting partner (fallback)", "business_id", businessID, "partner", partner.NomeSocio, "error", err)
						}
					}
				}
			}
		}
		
		// Commit transaction
		if err := tx.Commit(ctx); err != nil {
			slog.Warn("error committing transaction", "business_id", businessID, "error", err)
			tx.Rollback(ctx)
			continue
		}
	}
	
	return nil
}

// ImportPartnersBatch imports multiple CNPJs' partners data in a single batch operation
// This function processes all CNPJs together, making batch SELECTs for better performance
func (p *PostgreSQL) ImportPartnersBatch(batch map[string][]transform.PartnerData) error {
	ctx := context.Background()
	businessTable := fmt.Sprintf("%s.business", p.schema)
	sociosCnpjTable := fmt.Sprintf("%s.socios_cnpj", p.schema)
	
	if len(batch) == 0 {
		return nil
	}
	
	// Separate CNPJs by type (8 digits = base, 14 digits = full)
	var baseCNPJs []string
	var fullCNPJs []string
	cnpjToPartners := make(map[string][]transform.PartnerData)
	
	for cnpj, partners := range batch {
		cleanCNPJ := removeNonDigits(cnpj)
		cnpjToPartners[cleanCNPJ] = partners
		
		if len(cleanCNPJ) == 8 {
			baseCNPJs = append(baseCNPJs, cleanCNPJ)
		} else if len(cleanCNPJ) == 14 {
			fullCNPJs = append(fullCNPJs, cleanCNPJ)
		}
	}
	
	// Map to store CNPJ -> []businessID
	cnpjToBusinessIDs := make(map[string][]int)
	
	// Batch query for base CNPJs (8 digits)
	if len(baseCNPJs) > 0 {
		// Build query with IN clause for batch lookup
		placeholders := make([]string, len(baseCNPJs))
		args := make([]interface{}, len(baseCNPJs))
		for i, base := range baseCNPJs {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = base
		}
		
		query := fmt.Sprintf("SELECT id, LEFT(cnpj, 8) as cnpj_base FROM %s WHERE LEFT(cnpj, 8) IN (%s)", 
			businessTable, strings.Join(placeholders, ","))
		
		rows, err := p.pool.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("error batch querying businesses for CNPJ bases: %w", err)
		}
		defer rows.Close()
		
		for rows.Next() {
			var businessID int
			var cnpjBase string
			if err := rows.Scan(&businessID, &cnpjBase); err != nil {
				return fmt.Errorf("error scanning business_id: %w", err)
			}
			cnpjToBusinessIDs[cnpjBase] = append(cnpjToBusinessIDs[cnpjBase], businessID)
		}
		
		if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating business rows: %w", err)
		}
	}
	
	// Batch query for full CNPJs (14 digits)
	if len(fullCNPJs) > 0 {
		placeholders := make([]string, len(fullCNPJs))
		args := make([]interface{}, len(fullCNPJs))
		for i, full := range fullCNPJs {
			placeholders[i] = fmt.Sprintf("$%d", i+1)
			args[i] = full
		}
		
		query := fmt.Sprintf("SELECT id, cnpj FROM %s WHERE cnpj IN (%s)", 
			businessTable, strings.Join(placeholders, ","))
		
		rows, err := p.pool.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("error batch querying businesses for full CNPJs: %w", err)
		}
		defer rows.Close()
		
		for rows.Next() {
			var businessID int
			var cnpj string
			if err := rows.Scan(&businessID, &cnpj); err != nil {
				return fmt.Errorf("error scanning business_id: %w", err)
			}
			cnpjToBusinessIDs[cnpj] = append(cnpjToBusinessIDs[cnpj], businessID)
		}
		
		if err := rows.Err(); err != nil {
			return fmt.Errorf("error iterating business rows: %w", err)
		}
	}
	
	// Batch fetch all CNPJs for businesses found
	businessIDToCNPJ := make(map[int]string)
	if len(cnpjToBusinessIDs) > 0 {
		allBusinessIDs := make([]int, 0)
		for _, ids := range cnpjToBusinessIDs {
			allBusinessIDs = append(allBusinessIDs, ids...)
		}
		
		// Remove duplicates
		businessIDMap := make(map[int]bool)
		uniqueBusinessIDs := make([]int, 0)
		for _, id := range allBusinessIDs {
			if !businessIDMap[id] {
				businessIDMap[id] = true
				uniqueBusinessIDs = append(uniqueBusinessIDs, id)
			}
		}
		
		if len(uniqueBusinessIDs) > 0 {
			placeholders := make([]string, len(uniqueBusinessIDs))
			args := make([]interface{}, len(uniqueBusinessIDs))
			for i, id := range uniqueBusinessIDs {
				placeholders[i] = fmt.Sprintf("$%d", i+1)
				args[i] = id
			}
			
			query := fmt.Sprintf("SELECT id, cnpj FROM %s WHERE id IN (%s)", 
				businessTable, strings.Join(placeholders, ","))
			
			rows, err := p.pool.Query(ctx, query, args...)
			if err != nil {
				return fmt.Errorf("error batch fetching CNPJs for business_ids: %w", err)
			}
			defer rows.Close()
			
			for rows.Next() {
				var businessID int
				var cnpj string
				if err := rows.Scan(&businessID, &cnpj); err != nil {
					return fmt.Errorf("error scanning CNPJ: %w", err)
				}
				businessIDToCNPJ[businessID] = cnpj
			}
			
			if err := rows.Err(); err != nil {
				return fmt.Errorf("error iterating CNPJ rows: %w", err)
			}
		}
	}
	
	// Process all partners in batch transactions
	// Group by businessID to minimize transactions
	businessIDToPartners := make(map[int][]transform.PartnerData)
	businessIDToFullCNPJ := make(map[int]string)
	
	for cnpj, partners := range cnpjToPartners {
		businessIDs := cnpjToBusinessIDs[cnpj]
		if len(businessIDs) == 0 {
			slog.Warn("No businesses found for CNPJ, skipping partners", "cnpj", cnpj)
			continue
		}
		
		for _, businessID := range businessIDs {
			fullCNPJ, ok := businessIDToCNPJ[businessID]
			if !ok {
				slog.Warn("CNPJ not found for business_id", "business_id", businessID)
				continue
			}
			
			businessIDToPartners[businessID] = append(businessIDToPartners[businessID], partners...)
			businessIDToFullCNPJ[businessID] = fullCNPJ
		}
	}
	
	// Process each business in a transaction
	for businessID, partners := range businessIDToPartners {
		fullCNPJ := businessIDToFullCNPJ[businessID]
		
		// Begin transaction
		tx, err := p.pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("error beginning transaction: %w", err)
		}
		
		// Optimize transaction for bulk loading
		if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = OFF"); err != nil {
			slog.Warn("Could not disable synchronous commit", "error", err)
		}
		if _, err := tx.Exec(ctx, "SET LOCAL work_mem = '64MB'"); err != nil {
			slog.Warn("Could not set work_mem", "error", err)
		}
		
		// Use batch insert for better performance (no DELETE - preserve existing data)
		if len(partners) > 0 {
			// Prepare batch insert statement
			insertSQL := fmt.Sprintf(`INSERT INTO %s (
				business_id, cnpj, nome_socio, cpf_socio, data_entrada_sociedade, qualificacao
			) VALUES `, sociosCnpjTable)
			
			// Build values and args for batch insert, filtering duplicates
			values := make([]string, 0, len(partners))
			args := make([]interface{}, 0, len(partners)*6)
			argIndex := 1
			
			for _, partner := range partners {
				var dataEntrada *time.Time
				if partner.DataEntradaSociedade != nil {
					dt := time.Time(*partner.DataEntradaSociedade)
					dataEntrada = &dt
				}
				
				var qualificacao string
				if partner.QualificaoSocio != nil {
					qualificacao = *partner.QualificaoSocio
				}
				
				// Extract CPF from partner (may be masked like ***220050**)
				cpfSocio := partner.CNPJCPFDoSocio
				cleanCPFSocio := removeNonDigits(cpfSocio)
				// Se tiver mais de 11 dígitos (CNPJ), não insere
				if len(cleanCPFSocio) > 11 {
					slog.Warn("CPF com mais de 11 dígitos, ignorando sócio", "business_id", businessID, "cpf", cleanCPFSocio, "partner", partner.NomeSocio)
					continue // Pula este sócio se tiver mais de 11 dígitos
				}
				
				var cpfSocioValue interface{}
				if cleanCPFSocio != "" {
					cpfSocioValue = cleanCPFSocio
				} else {
					cpfSocioValue = nil
				}
				
				// Truncate partner fields that may exceed database limits
				nomeSocio := truncateTo120(partner.NomeSocio)
				qualificacaoTrunc := truncateTo120(qualificacao)
				
				values = append(values, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)", 
					argIndex, argIndex+1, argIndex+2, argIndex+3, argIndex+4, argIndex+5))
				args = append(args, businessID, fullCNPJ, nomeSocio, cpfSocioValue, dataEntrada, qualificacaoTrunc)
				argIndex += 6
			}
			
			// Execute batch insert with ON CONFLICT to skip duplicates
			if len(values) > 0 {
				insertSQL += strings.Join(values, ", ")
				insertSQL += fmt.Sprintf(` ON CONFLICT (cnpj, nome_socio) DO NOTHING`)
				_, err = tx.Exec(ctx, insertSQL, args...)
				if err != nil {
					slog.Warn("error batch inserting partners", "business_id", businessID, "partners_count", len(partners), "error", err)
					tx.Rollback(ctx)
					continue
				}
			}
		}
		
		// Commit transaction
		if err := tx.Commit(ctx); err != nil {
			slog.Warn("error committing transaction", "business_id", businessID, "error", err)
			tx.Rollback(ctx)
			continue
		}
	}
	
	return nil
}

// MetaSave saves a key/value pair in the metadata table.
func (p *PostgreSQL) MetaSave(k, v string) error {
	if len(k) > 16 {
		return fmt.Errorf("metatable can only take keys that are at maximum 16 chars long")
	}
	s, err := p.renderTemplate("meta_save")
	if err != nil {
		return fmt.Errorf("error rendering meta-save template: %w", err)
	}
	if _, err := p.pool.Exec(context.Background(), s, k, v); err != nil {
		return fmt.Errorf("error saving %s to metadata: %w", k, err)
	}
	return nil
}

// MetaRead reads a key/value pair from the metadata table.
func (p *PostgreSQL) MetaRead(k string) (string, error) {
	rows, err := p.pool.Query(context.Background(), p.metaReadQuery, k)
	if err != nil {
		return "", fmt.Errorf("error looking for metadata key %s: %w", k, err)
	}
	v, err := pgx.CollectOneRow(rows, pgx.RowTo[string])
	if err != nil {
		return "", fmt.Errorf("error reading for metadata key %s: %w", k, err)
	}
	return v, nil
}

// CreateExtraIndexes responsible for creating additional indexes in the database
func (p *PostgreSQL) CreateExtraIndexes(idxs []string) error {
	if err := transform.ValidateIndexes(idxs); err != nil {
		return fmt.Errorf("index name error: %w", err)
	}
	for _, idx := range idxs {
		i := ExtraIndex{
			IsRoot: !strings.Contains(idx, "."),
			Name:   fmt.Sprintf("json.%s", idx),
			Value:  idx,
		}
		p.ExtraIndexes = append(p.ExtraIndexes, i)
	}
	s, err := p.renderTemplate("extra_indexes")
	if err != nil {
		return fmt.Errorf("error rendering extra-indexes template: %w", err)
	}
	if _, err := p.pool.Exec(context.Background(), s); err != nil {
		return fmt.Errorf("expected the error to create indexe: %w", err)
	}
	slog.Info(fmt.Sprintf("%d Indexes successfully created in the table %s", len(idxs), p.CompanyTableName))
	return nil
}

// NewPostgreSQL creates a new PostgreSQL connection and ping it to make sure it works.
func NewPostgreSQL(uri, schema string) (PostgreSQL, error) {
	cfg, err := pgxpool.ParseConfig(uri)
	if err != nil {
		return PostgreSQL{}, fmt.Errorf("could not create database config: %w", err)
	}
	cfg.MaxConns = 128
	cfg.MinConns = 1
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.MaxConnLifetime = 30 * time.Minute
	conn, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return PostgreSQL{}, fmt.Errorf("could not connect to the database: %w", err)
	}
	p := PostgreSQL{
		pool:             conn,
		uri:              uri,
		schema:           schema,
		CompanyTableName: companyTableName,
		MetaTableName:    metaTableName,
		CursorFieldName:  cursorFieldName,
		IDFieldName:      idFieldName,
		JSONFieldName:    jsonFieldName,
		KeyFieldName:     keyFieldName,
		ValueFieldName:   valueFieldName,
	}
	p.getCompanyQuery, err = p.renderTemplate("get")
	if err != nil {
		return PostgreSQL{}, fmt.Errorf("error rendering get template: %w", err)
	}
	p.metaReadQuery, err = p.renderTemplate("meta_read")
	if err != nil {
		return PostgreSQL{}, fmt.Errorf("error rendering meta-read template: %w", err)
	}
	if err := p.pool.Ping(context.Background()); err != nil {
		return PostgreSQL{}, fmt.Errorf("could not connect to postgres: %w", err)
	}
	return p, nil
}
