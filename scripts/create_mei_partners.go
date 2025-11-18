package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"

	"github.com/cuducos/minha-receita/db"
	"github.com/jackc/pgx/v5"
	"github.com/schollz/progressbar/v3"
)

const (
	batchSize = 10000
	qualificacaoMEI = "Sócio Administrador"
)

// removeNonDigits removes all non-digit characters from a string
func removeNonDigits(s string) string {
	re := regexp.MustCompile(`\D`)
	return re.ReplaceAllString(s, "")
}

// truncateString truncates a string to the specified maximum length
func truncateString(s string, maxLen int) string {
	if maxLen <= 0 {
		return s
	}
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return s[:maxLen]
	}
	return s[:maxLen-3] + "..."
}

// truncateTo120 truncates a string to 120 characters
func truncateTo120(s string) string {
	return truncateString(s, 120)
}

// extractCPFFromRazaoSocial extracts CPF (11 digits) from the end of razao_social
// Returns: nome (without CPF), cpf (11 digits), found (true if CPF was found)
func extractCPFFromRazaoSocial(razaoSocial string) (nome string, cpf string, found bool) {
	// Pattern: ends with space followed by 11 digits
	re := regexp.MustCompile(`^(.+?)\s+([0-9]{11})$`)
	matches := re.FindStringSubmatch(strings.TrimSpace(razaoSocial))
	
	if len(matches) == 3 {
		nome = strings.TrimSpace(matches[1])
		cpf = matches[2]
		found = true
		return
	}
	
	return razaoSocial, "", false
}

type MEIPartner struct {
	BusinessID int64
	CNPJ       string
	NomeSocio  string
	CPFSocio   string
}

func main() {
	// Setup logging
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	// Get database connection
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		slog.Error("DATABASE_URL environment variable not set")
		os.Exit(1)
	}

	schema := os.Getenv("POSTGRES_SCHEMA")
	if schema == "" {
		schema = "public"
	}

	// Connect to database
	pg, err := db.NewPostgreSQL(databaseURL, schema)
	if err != nil {
		slog.Error("Failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pg.Close()

	// Open CSV file
	csvFile := "/root/minha-receita/empresas_sem_socios.csv"
	file, err := os.Open(csvFile)
	if err != nil {
		slog.Error("Failed to open CSV file", "file", csvFile, "error", err)
		os.Exit(1)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	
	// Read header
	header, err := reader.Read()
	if err != nil {
		slog.Error("Failed to read CSV header", "error", err)
		os.Exit(1)
	}
	slog.Info("CSV header", "header", header)

	// Count total lines for progress bar (estimate)
	fileInfo, err := file.Stat()
	if err != nil {
		slog.Warn("Could not get file info for progress estimation", "error", err)
	}
	
	// Estimate: average CSV line is ~100 bytes, but we'll use a conservative estimate
	var totalLines int64 = -1 // -1 means unknown
	if fileInfo != nil {
		// Rough estimate: file size / 150 bytes per line
		totalLines = fileInfo.Size() / 150
		if totalLines < 1000 {
			totalLines = -1 // Too small, use unknown mode
		}
	}

	// Create progress bar
	bar := progressbar.NewOptions64(
		totalLines,
		progressbar.OptionSetDescription("Processando empresas"),
		progressbar.OptionSetWidth(60),
		progressbar.OptionShowBytes(false),
		progressbar.OptionShowCount(),
		progressbar.OptionShowElapsedTimeOnFinish(),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "=",
			SaucerHead:    ">",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}),
		progressbar.OptionOnCompletion(func() {
			fmt.Print("\n")
		}),
	)

	// Statistics
	var (
		totalProcessed   int
		cpfFound         int
		businessFound    int
		successfullyInserted int
		errors           int
	)

	// Batch processing
	var batch []MEIPartner
	ctx := context.Background()
	businessTable := fmt.Sprintf("%s.business", schema)
	sociosTable := fmt.Sprintf("%s.socios_cnpj", schema)

	// Process CSV line by line
	for {
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			slog.Warn("Error reading CSV line", "error", err)
			continue
		}

		if len(record) < 3 {
			slog.Warn("Invalid CSV record", "record", record)
			continue
		}

		totalProcessed++

		// Update progress bar
		if totalLines > 0 {
			bar.Set64(int64(totalProcessed))
		} else {
			bar.Add(1)
		}

		// Extract CNPJ and razao_social
		cnpj := record[1]
		razaoSocial := record[2]

		// Extract CPF from razao_social
		nomeSocio, cpfSocio, found := extractCPFFromRazaoSocial(razaoSocial)
		if !found {
			continue // Skip if no CPF found
		}

		cpfFound++

		// Clean CNPJ
		cleanCNPJ := removeNonDigits(cnpj)
		if len(cleanCNPJ) != 14 {
			slog.Warn("Invalid CNPJ length", "cnpj", cleanCNPJ)
			continue
		}

		// Find business_id
		var businessID int64
		err = pg.Pool().QueryRow(ctx, fmt.Sprintf("SELECT id FROM %s WHERE cnpj = $1", businessTable), cleanCNPJ).Scan(&businessID)
		if err != nil {
			if err == pgx.ErrNoRows {
				slog.Warn("Business not found", "cnpj", cleanCNPJ)
				continue
			}
			slog.Warn("Error querying business", "cnpj", cleanCNPJ, "error", err)
			errors++
			continue
		}

		businessFound++

		// Clean and truncate CPF
		cleanCPF := removeNonDigits(cpfSocio)
		if len(cleanCPF) > 11 {
			cleanCPF = cleanCPF[:11]
		}
		if len(cleanCPF) != 11 {
			slog.Warn("Invalid CPF length", "cpf", cleanCPF, "cnpj", cleanCNPJ)
			errors++
			continue
		}

		// Truncate nome
		nomeTrunc := truncateTo120(nomeSocio)

		// Add to batch
		batch = append(batch, MEIPartner{
			BusinessID: businessID,
			CNPJ:       cleanCNPJ,
			NomeSocio:  nomeTrunc,
			CPFSocio:   cleanCPF,
		})

		// Process batch when it reaches batchSize
		if len(batch) >= batchSize {
			inserted, errCount := insertBatch(ctx, &pg, batch, sociosTable)
			successfullyInserted += inserted
			errors += errCount
			batch = batch[:0] // Clear batch
			
			// Update progress bar description with stats
			bar.Describe(fmt.Sprintf("Processando: %d processadas | %d CPFs | %d inseridos | %d erros", 
				totalProcessed, cpfFound, successfullyInserted, errors))
		}
	}

	// Process remaining batch
	if len(batch) > 0 {
		inserted, errCount := insertBatch(ctx, &pg, batch, sociosTable)
		successfullyInserted += inserted
		errors += errCount
	}

	// Finish progress bar
	bar.Finish()

	// Final statistics
	slog.Info("Processing completed",
		"total_processed", totalProcessed,
		"cpf_found", cpfFound,
		"business_found", businessFound,
		"successfully_inserted", successfullyInserted,
		"errors", errors,
	)
}

// insertBatch inserts a batch of MEI partners into socios_cnpj table
func insertBatch(ctx context.Context, pg *db.PostgreSQL, batch []MEIPartner, sociosTable string) (inserted int, errors int) {
	if len(batch) == 0 {
		return 0, 0
	}

	// Begin transaction
	tx, err := pg.Pool().Begin(ctx)
	if err != nil {
		slog.Error("Failed to begin transaction", "error", err)
		return 0, len(batch)
	}
	defer tx.Rollback(ctx)

	// Optimize transaction for bulk loading
	if _, err := tx.Exec(ctx, "SET LOCAL synchronous_commit = OFF"); err != nil {
		slog.Warn("Could not disable synchronous commit", "error", err)
	}
	if _, err := tx.Exec(ctx, "SET LOCAL work_mem = '64MB'"); err != nil {
		slog.Warn("Could not set work_mem", "error", err)
	}

	// Build batch insert SQL
	insertSQL := fmt.Sprintf(`INSERT INTO %s (
		business_id, cnpj, nome_socio, cpf_socio, data_entrada_sociedade, qualificacao
	) VALUES `, sociosTable)

	values := make([]string, 0, len(batch))
	args := make([]interface{}, 0, len(batch)*6)
	argIndex := 1

	for _, partner := range batch {
		values = append(values, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d)",
			argIndex, argIndex+1, argIndex+2, argIndex+3, argIndex+4, argIndex+5))
		args = append(args,
			partner.BusinessID,  // business_id
			partner.CNPJ,       // cnpj
			partner.NomeSocio,  // nome_socio
			partner.CPFSocio,   // cpf_socio
			nil,                // data_entrada_sociedade (NULL)
			qualificacaoMEI,    // qualificacao
		)
		argIndex += 6
	}

	// Execute batch insert
	if len(values) > 0 {
		insertSQL += strings.Join(values, ", ")
		_, err = tx.Exec(ctx, insertSQL, args...)
		if err != nil {
			slog.Error("Failed to insert batch", "error", err, "batch_size", len(batch))
			return 0, len(batch)
		}
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		slog.Error("Failed to commit transaction", "error", err)
		return 0, len(batch)
	}

	return len(batch), 0
}

