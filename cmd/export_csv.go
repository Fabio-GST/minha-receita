package cmd

import (
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/cuducos/minha-receita/db"
	"github.com/cuducos/minha-receita/transform"
	"github.com/spf13/cobra"
)

const exportCSVHelper = `
Process the downloaded Federal Revenue files and export two CSV files ready
for bulk insertion into the business and socios_cnpj tables:

  - business.csv      → one row per CNPJ with all company fields
  - socios_cnpj.csv   → one row per partner (QSA entry)

After importing both CSVs, link partners to companies by running:

  UPDATE socios_cnpj s
  SET business_id = b.id
  FROM business b
  WHERE s.cnpj = b.cnpj AND s.business_id IS NULL;

This command does NOT require a database connection.
`

var (
	exportBusinessPath string
	exportSociosPath   string
	exportNoPrivacy    bool
)

var exportCSVCmd = &cobra.Command{
	Use:   "export-csv",
	Short: "Exports processed CNPJ data to CSV files for bulk insertion",
	Long:  exportCSVHelper,
	RunE: func(_ *cobra.Command, _ []string) error {
		if err := assertDirExists(); err != nil {
			return err
		}

		exporter := db.NewCSVExporter(exportBusinessPath, exportSociosPath)
		defer exporter.Close()

		slog.Info("Starting CSV export", "business", exportBusinessPath, "socios", exportSociosPath)

		if err := transform.Transform(
			dir,
			exporter,
			transform.MaxParallelDBQueries,
			transform.MaxParallelKVWrites,
			transform.BatchSize,
			!exportNoPrivacy,
			true, // always use structured mode for CSV export
		); err != nil {
			return fmt.Errorf("error during CSV export: %w", err)
		}

		slog.Info("CSV export completed", "business", exportBusinessPath, "socios", exportSociosPath)
		fmt.Printf("\nFiles generated:\n  %s\n  %s\n\n", exportBusinessPath, exportSociosPath)
		fmt.Println("To link socios to business after import, run:")
		fmt.Println("  UPDATE socios_cnpj s SET business_id = b.id FROM business b WHERE s.cnpj = b.cnpj AND s.business_id IS NULL;")
		return nil
	},
}

func exportCSVCLI() *cobra.Command {
	exportCSVCmd = addDataDir(exportCSVCmd)
	exportCSVCmd.Flags().StringVar(
		&exportBusinessPath,
		"business-output",
		filepath.Join("data", "business.csv"),
		"output path for the business CSV file",
	)
	exportCSVCmd.Flags().StringVar(
		&exportSociosPath,
		"socios-output",
		filepath.Join("data", "socios_cnpj.csv"),
		"output path for the socios_cnpj CSV file",
	)
	exportCSVCmd.Flags().BoolVar(
		&exportNoPrivacy,
		"no-privacy",
		false,
		"include email addresses, CPF and other PII in the CSV data",
	)
	return exportCSVCmd
}
