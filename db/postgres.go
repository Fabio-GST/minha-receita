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
func (p *PostgreSQL) CreateCompaniesStructuredDirect(batch []transform.Company) error {
	ctx := context.Background()
	
	// Begin transaction with optimized settings for bulk loading
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	
	// Optimize transaction for bulk loading
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = OFF"); err != nil {
		slog.Warn("Could not disable synchronous commit", "error", err)
	}
	if _, err := tx.Exec(ctx, "SET LOCAL work_mem = '256MB'"); err != nil {
		slog.Warn("Could not set work_mem", "error", err)
	}

	// Prepare statements
	insertBusinessSQL := `INSERT INTO business (
		cnpj, razao_social, nome_fantasia, situacao_cadastral,
		cnae_principal, tipo_cnae_principal, cnaes_secundarios,
		capital_social, natureza_juridica, qualificacao_responsavel,
		porte_empresa, identificador_matriz_filial,
		data_situacao_cadastral, motivo_situacao_cadastral,
		data_inicio_atividade, email,
		endereco_cep, endereco_numero, endereco_logradouro,
		endereco_bairro, endereco_cidade, endereco_uf,
		endereco_tipo, endereco_complemento, telefones
	) VALUES (
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
		$11, $12, $13, $14, $15, $16, $17, $18, $19,
		$20, $21, $22, $23, $24, $25
	)
	ON CONFLICT (cnpj) 
	DO UPDATE SET 
		razao_social = EXCLUDED.razao_social,
		nome_fantasia = EXCLUDED.nome_fantasia,
		situacao_cadastral = EXCLUDED.situacao_cadastral,
		cnae_principal = EXCLUDED.cnae_principal,
		tipo_cnae_principal = EXCLUDED.tipo_cnae_principal,
		cnaes_secundarios = EXCLUDED.cnaes_secundarios,
		capital_social = EXCLUDED.capital_social,
		natureza_juridica = EXCLUDED.natureza_juridica,
		qualificacao_responsavel = EXCLUDED.qualificacao_responsavel,
		porte_empresa = EXCLUDED.porte_empresa,
		identificador_matriz_filial = EXCLUDED.identificador_matriz_filial,
		data_situacao_cadastral = EXCLUDED.data_situacao_cadastral,
		motivo_situacao_cadastral = EXCLUDED.motivo_situacao_cadastral,
		data_inicio_atividade = EXCLUDED.data_inicio_atividade,
		email = EXCLUDED.email,
		endereco_cep = EXCLUDED.endereco_cep,
		endereco_numero = EXCLUDED.endereco_numero,
		endereco_logradouro = EXCLUDED.endereco_logradouro,
		endereco_bairro = EXCLUDED.endereco_bairro,
		endereco_cidade = EXCLUDED.endereco_cidade,
		endereco_uf = EXCLUDED.endereco_uf,
		endereco_tipo = EXCLUDED.endereco_tipo,
		endereco_complemento = EXCLUDED.endereco_complemento,
		telefones = EXCLUDED.telefones,
		updated_at = now()
	RETURNING id`

	deletePartnersSQL := `DELETE FROM socios_cnpj WHERE business_id = $1`

	for _, company := range batch {
		// Clean CNPJ
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

		// Insert/Update business
		var businessID int64
		err := tx.QueryRow(ctx, insertBusinessSQL,
			cleanCNPJ,
			company.RazaoSocial,
			company.NomeFantasia,
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
			company.Logradouro,
			company.Bairro,
			municipio,
			company.UF,
			enderecoTipo,
			company.Complemento,
			formatPhones(&company),
		).Scan(&businessID)

		if err != nil {
			slog.Error("error inserting business", "cnpj", cleanCNPJ, "error", err)
			continue
		}

		// Delete existing partners
		_, err = tx.Exec(ctx, deletePartnersSQL, businessID)
		if err != nil {
			slog.Warn("error deleting existing partners", "business_id", businessID, "error", err)
		}

		// Insert partners
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
			cleanCPFSocio := removeNonDigits(cpfSocio)
			if len(cleanCPFSocio) != 11 {
				cleanCPFSocio = ""
			}

			var cpfSocioValue interface{}
			if cleanCPFSocio != "" {
				cpfSocioValue = cleanCPFSocio
			} else {
				cpfSocioValue = nil
			}

			_, err = tx.Exec(ctx, `INSERT INTO socios_cnpj (
				business_id, cnpj, nome_socio, cpf_socio, data_entrada_sociedade, qualificacao
			) VALUES ($1, $2, $3, $4, $5, $6)`,
				businessID,
				cleanCNPJ,
				partner.NomeSocio,
				cpfSocioValue,
				dataEntrada,
				qualificacao,
			)
			if err != nil {
				slog.Warn("error inserting partner", "business_id", businessID, "partner", partner.NomeSocio, "error", err)
			}
		}
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}

// CreateCompaniesStructured inserts companies into business and business_partners tables
// This method creates JSON first (for compatibility), but CreateCompaniesStructuredDirect is preferred
func (p *PostgreSQL) CreateCompaniesStructured(batch [][]string) error {
	ctx := context.Background()
	
	// Begin transaction with optimized settings for bulk loading
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}
	defer tx.Rollback(ctx)
	
	// Optimize transaction for bulk loading: disable synchronous commit temporarily
	// This reduces I/O and improves performance during bulk inserts
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = OFF"); err != nil {
		slog.Warn("Could not disable synchronous commit", "error", err)
		// Continue anyway
	}
	
	// Set work_mem higher for this transaction to reduce disk usage
	if _, err := tx.Exec(ctx, "SET LOCAL work_mem = '256MB'"); err != nil {
		slog.Warn("Could not set work_mem", "error", err)
		// Continue anyway
	}

	// Prepare statements
	insertBusinessSQL := `INSERT INTO business (
		cnpj, razao_social, nome_fantasia, situacao_cadastral,
		cnae_principal, tipo_cnae_principal, cnaes_secundarios,
		capital_social, natureza_juridica, qualificacao_responsavel,
		porte_empresa, identificador_matriz_filial,
		data_situacao_cadastral, motivo_situacao_cadastral,
		data_inicio_atividade, email,
		endereco_cep, endereco_numero, endereco_logradouro,
		endereco_bairro, endereco_cidade, endereco_uf,
		endereco_tipo, endereco_complemento, telefones
	) VALUES (
		$1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
		$11, $12, $13, $14, $15, $16, $17, $18, $19,
		$20, $21, $22, $23, $24, $25
	)
	ON CONFLICT (cnpj) 
	DO UPDATE SET 
		razao_social = EXCLUDED.razao_social,
		nome_fantasia = EXCLUDED.nome_fantasia,
		situacao_cadastral = EXCLUDED.situacao_cadastral,
		cnae_principal = EXCLUDED.cnae_principal,
		tipo_cnae_principal = EXCLUDED.tipo_cnae_principal,
		cnaes_secundarios = EXCLUDED.cnaes_secundarios,
		capital_social = EXCLUDED.capital_social,
		natureza_juridica = EXCLUDED.natureza_juridica,
		qualificacao_responsavel = EXCLUDED.qualificacao_responsavel,
		porte_empresa = EXCLUDED.porte_empresa,
		identificador_matriz_filial = EXCLUDED.identificador_matriz_filial,
		data_situacao_cadastral = EXCLUDED.data_situacao_cadastral,
		motivo_situacao_cadastral = EXCLUDED.motivo_situacao_cadastral,
		data_inicio_atividade = EXCLUDED.data_inicio_atividade,
		email = EXCLUDED.email,
		endereco_cep = EXCLUDED.endereco_cep,
		endereco_numero = EXCLUDED.endereco_numero,
		endereco_logradouro = EXCLUDED.endereco_logradouro,
		endereco_bairro = EXCLUDED.endereco_bairro,
		endereco_cidade = EXCLUDED.endereco_cidade,
		endereco_uf = EXCLUDED.endereco_uf,
		endereco_tipo = EXCLUDED.endereco_tipo,
		endereco_complemento = EXCLUDED.endereco_complemento,
		telefones = EXCLUDED.telefones,
		updated_at = now()
	RETURNING id`

	// Delete existing partners for companies being updated
	deletePartnersSQL := `DELETE FROM socios_cnpj WHERE business_id = $1`

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

		// Insert/Update business
		var businessID int64
		err := tx.QueryRow(ctx, insertBusinessSQL,
			cleanCNPJ,                                    // cnpj
			company.RazaoSocial,                         // razao_social
			company.NomeFantasia,                        // nome_fantasia
			situacaoCadastralToString(company.SituacaoCadastral), // situacao_cadastral
			formatCNAEPrincipal(company.CNAEFiscal),     // cnae_principal
			getStringValue(company.CNAEFiscalDescricao), // tipo_cnae_principal
			formatSecondaryCNAEs(company.CNAESecundarios), // cnaes_secundarios
			capitalSocial,                               // capital_social
			naturezaJuridica,                            // natureza_juridica
			qualificacaoResponsavel,                      // qualificacao_responsavel
			porteEmpresa,                                // porte_empresa
			identificadorMatrizFilial,                   // identificador_matriz_filial
			convertDate(company.DataSituacaoCadastral),  // data_situacao_cadastral
			motivoSituacaoCadastral,                     // motivo_situacao_cadastral
			convertDate(company.DataInicioAtividade),     // data_inicio_atividade
			getStringValue(company.Email),               // email
			cleanCEP,                                    // endereco_cep
			company.Numero,                              // endereco_numero
			company.Logradouro,                          // endereco_logradouro
			company.Bairro,                              // endereco_bairro
			municipio,                                   // endereco_cidade
			company.UF,                                  // endereco_uf
			enderecoTipo,                                // endereco_tipo
			company.Complemento,                         // endereco_complemento
			formatPhones(&company),                      // telefones
		).Scan(&businessID)

		if err != nil {
			slog.Error("error inserting business", "cnpj", cleanCNPJ, "error", err)
			continue
		}

		// Delete existing partners before inserting new ones
		// This ensures we don't have duplicates and handles NULL people_id correctly
		_, err = tx.Exec(ctx, deletePartnersSQL, businessID)
		if err != nil {
			slog.Warn("error deleting existing partners", "business_id", businessID, "error", err)
			// Continue anyway
		}

		// Insert partners into socios_cnpj table
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
			cleanCPFSocio := removeNonDigits(cpfSocio)
			// If CPF is not valid (not 11 digits), set to NULL
			if len(cleanCPFSocio) != 11 {
				cleanCPFSocio = ""
			}

			// Insert partner into socios_cnpj
			var cpfSocioValue interface{}
			if cleanCPFSocio != "" {
				cpfSocioValue = cleanCPFSocio
			} else {
				cpfSocioValue = nil
			}

			_, err = tx.Exec(ctx, `INSERT INTO socios_cnpj (
				business_id, cnpj, nome_socio, cpf_socio, data_entrada_sociedade, qualificacao
			) VALUES ($1, $2, $3, $4, $5, $6)`,
				businessID,    // business_id
				cleanCNPJ,     // cnpj (CNPJ da empresa)
				partner.NomeSocio, // nome_socio
				cpfSocioValue, // cpf_socio (pode ser NULL)
				dataEntrada,   // data_entrada_sociedade
				qualificacao,  // qualificacao
			)
			if err != nil {
				slog.Warn("error inserting partner", "business_id", businessID, "partner", partner.NomeSocio, "error", err)
				// Continue with next partner
			}
		}
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
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
	slog.Info("Running VACUUM to optimize storage before loading...")
	vacuumSQL := "VACUUM"
	if _, err := p.pool.Exec(context.Background(), vacuumSQL); err != nil {
		slog.Warn("Could not run VACUUM before loading", "error", err)
		// Continue anyway, not critical - VACUUM can fail if disk is full
	} else {
		slog.Info("VACUUM completed successfully")
		// Run ANALYZE separately to update statistics
		analyzeSQL := "ANALYZE"
		if _, err := p.pool.Exec(context.Background(), analyzeSQL); err != nil {
			slog.Warn("Could not run ANALYZE", "error", err)
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
	
	// Check which table exists: business (structured) or cnpj (JSON)
	// Try business table first (structured mode)
	businessTable := fmt.Sprintf("%s.business", p.schema)
	cnpjTable := p.CompanyTableFullName()
	
	var checkQuery string
	
	// Check if business table exists
	checkQuery = `SELECT EXISTS (
		SELECT FROM information_schema.tables 
		WHERE table_schema = $1 AND table_name = 'business'
	)`
	var businessExists bool
	err := p.pool.QueryRow(context.Background(), checkQuery, p.schema).Scan(&businessExists)
	if err != nil {
		return fmt.Errorf("error checking if business table exists: %w", err)
	}
	
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
		sql := fmt.Sprintf("ALTER TABLE %s SET UNLOGGED", businessTable)
		if _, err := p.pool.Exec(context.Background(), sql); err != nil {
			return fmt.Errorf("error during pre load: %s\n%w", sql, err)
		}
		slog.Debug("Set business table to UNLOGGED", "table", businessTable)
		
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
		sql := fmt.Sprintf("ALTER TABLE %s SET UNLOGGED", cnpjTable)
		if _, err := p.pool.Exec(context.Background(), sql); err != nil {
			return fmt.Errorf("error during pre load: %s\n%w", sql, err)
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
