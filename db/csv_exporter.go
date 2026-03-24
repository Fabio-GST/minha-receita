package db

import (
	"context"
	"encoding/csv"
	"encoding/json/v2"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/cuducos/minha-receita/transform"
)

// CSVExporter implements the database interface and writes to CSV files instead
// of inserting into a database. It produces business.csv and socios_cnpj.csv
// ready for bulk insertion into the business and socios_cnpj tables.
//
// After loading both CSVs, run the following SQL to link socios to business:
//
//	UPDATE socios_cnpj s
//	SET business_id = b.id
//	FROM business b
//	WHERE s.cnpj = b.cnpj AND s.business_id IS NULL;
type CSVExporter struct {
	businessPath   string
	sociosPath     string
	businessFile   *os.File
	sociosFile     *os.File
	businessWriter *csv.Writer
	sociosWriter   *csv.Writer
}

// NewCSVExporter creates a new CSVExporter that will write to the given paths.
func NewCSVExporter(businessPath, sociosPath string) *CSVExporter {
	return &CSVExporter{
		businessPath: businessPath,
		sociosPath:   sociosPath,
	}
}

// Close flushes and closes the underlying CSV files.
func (e *CSVExporter) Close() {
	if e.businessWriter != nil {
		e.businessWriter.Flush()
	}
	if e.businessFile != nil {
		e.businessFile.Close()
	}
	if e.sociosWriter != nil {
		e.sociosWriter.Flush()
	}
	if e.sociosFile != nil {
		e.sociosFile.Close()
	}
}

// Create is a no-op for CSV export.
func (e *CSVExporter) Create() error { return nil }

// Drop is a no-op for CSV export.
func (e *CSVExporter) Drop() error { return nil }

// PreLoad creates the CSV files and writes the headers.
func (e *CSVExporter) PreLoad() error {
	var err error

	e.businessFile, err = os.Create(e.businessPath)
	if err != nil {
		return fmt.Errorf("error creating %s: %w", e.businessPath, err)
	}
	e.businessWriter = csv.NewWriter(e.businessFile)

	e.sociosFile, err = os.Create(e.sociosPath)
	if err != nil {
		return fmt.Errorf("error creating %s: %w", e.sociosPath, err)
	}
	e.sociosWriter = csv.NewWriter(e.sociosFile)

	businessHeader := []string{
		"cnpj", "razao_social", "nome_fantasia", "situacao_cadastral",
		"cnae_principal", "tipo_cnae_principal", "cnaes_secundarios",
		"capital_social", "natureza_juridica", "qualificacao_responsavel",
		"porte_empresa", "identificador_matriz_filial",
		"data_situacao_cadastral", "motivo_situacao_cadastral",
		"data_inicio_atividade", "email",
		"endereco_cep", "endereco_numero", "endereco_logradouro",
		"endereco_bairro", "endereco_cidade", "endereco_uf",
		"endereco_tipo", "endereco_complemento", "telefones",
	}
	if err := e.businessWriter.Write(businessHeader); err != nil {
		return fmt.Errorf("error writing business CSV header: %w", err)
	}

	sociosHeader := []string{
		"cnpj", "nome_socio", "cpf_socio", "data_entrada_sociedade", "qualificacao",
	}
	if err := e.sociosWriter.Write(sociosHeader); err != nil {
		return fmt.Errorf("error writing socios CSV header: %w", err)
	}

	return nil
}

// CreateCompanies is not supported for CSV export; use CreateCompaniesStructured.
func (e *CSVExporter) CreateCompanies(_ [][]string) error {
	return fmt.Errorf("CSV export requires --no-privacy and structured mode; use CreateCompaniesStructured")
}

// CreateCompaniesStructured writes company and partner rows to the CSV files.
// The batch format matches what the transform pipeline sends: each item is
// [cnpj, jsonString].
func (e *CSVExporter) CreateCompaniesStructured(batch [][]string) error {
	for _, record := range batch {
		if len(record) < 2 {
			slog.Warn("skipping invalid record", "record", record)
			continue
		}

		var company transform.Company
		if err := json.Unmarshal([]byte(record[1]), &company); err != nil {
			slog.Error("error parsing company JSON", "cnpj", record[0], "error", err)
			continue
		}

		cleanCNPJ := removeNonDigits(company.CNPJ)
		if len(cleanCNPJ) != 14 {
			slog.Warn("invalid CNPJ length, skipping", "cnpj", cleanCNPJ)
			continue
		}

		// --- business row ---

		var capitalSocial string
		if company.CapitalSocial != nil {
			capitalSocial = strconv.FormatFloat(float64(*company.CapitalSocial), 'f', 2, 64)
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

		var municipio string
		if company.Municipio != nil {
			municipio = *company.Municipio
		}

		var dataSituacao string
		if t := convertDate(company.DataSituacaoCadastral); t != nil {
			dataSituacao = t.Format("2006-01-02")
		}

		var dataInicio string
		if t := convertDate(company.DataInicioAtividade); t != nil {
			dataInicio = t.Format("2006-01-02")
		}

		businessRow := []string{
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
			dataSituacao,
			motivoSituacaoCadastral,
			dataInicio,
			getStringValue(company.Email),
			cleanCEP,
			company.Numero,
			company.Logradouro,
			company.Bairro,
			municipio,
			company.UF,
			"0", // endereco_tipo: default 0; the pipeline maps it to 0 for all cases
			company.Complemento,
			formatPhones(&company),
		}

		if err := e.businessWriter.Write(businessRow); err != nil {
			slog.Error("error writing business row", "cnpj", cleanCNPJ, "error", err)
		}

		// --- socios_cnpj rows ---

		for _, partner := range company.QuadroSocietario {
			var dataEntrada string
			if t := convertDate(partner.DataEntradaSociedade); t != nil {
				dataEntrada = t.Format("2006-01-02")
			}

			var qualificacao string
			if partner.QualificaoSocio != nil {
				qualificacao = *partner.QualificaoSocio
			}

			cpfSocio := removeNonDigits(partner.CNPJCPFDoSocio)
			if len(cpfSocio) != 11 {
				cpfSocio = ""
			}

			socioRow := []string{
				cleanCNPJ,
				partner.NomeSocio,
				cpfSocio,
				dataEntrada,
				qualificacao,
			}

			if err := e.sociosWriter.Write(socioRow); err != nil {
				slog.Error("error writing socio row", "cnpj", cleanCNPJ, "partner", partner.NomeSocio, "error", err)
			}
		}
	}

	e.businessWriter.Flush()
	e.sociosWriter.Flush()

	if err := e.businessWriter.Error(); err != nil {
		return fmt.Errorf("error flushing business CSV writer: %w", err)
	}
	if err := e.sociosWriter.Error(); err != nil {
		return fmt.Errorf("error flushing socios CSV writer: %w", err)
	}

	return nil
}

// PostLoad flushes and closes the CSV files.
func (e *CSVExporter) PostLoad() error {
	e.Close()
	return nil
}

// CreateExtraIndexes is a no-op for CSV export.
func (e *CSVExporter) CreateExtraIndexes(_ []string) error { return nil }

// MetaSave is a no-op for CSV export.
func (e *CSVExporter) MetaSave(_, _ string) error { return nil }

// MetaRead is not supported for CSV export.
func (e *CSVExporter) MetaRead(_ string) (string, error) {
	return "", fmt.Errorf("MetaRead not supported for CSV exporter")
}

// GetCompany is not supported for CSV export.
func (e *CSVExporter) GetCompany(_ string) (string, error) {
	return "", fmt.Errorf("GetCompany not supported for CSV exporter")
}

// Search is not supported for CSV export.
func (e *CSVExporter) Search(_ context.Context, _ *Query) (string, error) {
	return "", fmt.Errorf("Search not supported for CSV exporter")
}
