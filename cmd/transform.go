package cmd

import (
	"fmt"

	"github.com/cuducos/minha-receita/transform"
	"github.com/spf13/cobra"
)

const transformHelper = `
Convert the CSV files from the Federal Revenue for venues (Estabelecimentos*.zip
group of files) into records in the database, 1 record per CNPJ, joining
information from all other source CSV files.

The transformation process is divided into two steps:
1. Load relational data to a key-value store
2. Load the full database using the key-value store
`

var (
	maxParallelDBQueries int
	maxParallelKVWrites  int
	batchSize            int
	cleanUp              bool
	noPrivacy            bool
	structured           bool
)

var transformCmd = &cobra.Command{
	Use:   "transform",
	Short: "Transforms the CSV files into database records",
	Long:  transformHelper,
	RunE: func(_ *cobra.Command, _ []string) error {
		if err := assertDirExists(); err != nil {
			return err
		}
		db, err := loadDatabase()
		if err != nil {
			return fmt.Errorf("could not find database: %w", err)
		}
		defer db.Close()
		if cleanUp {
			err = db.Drop()
			if err != nil {
				return err
			}
			err = db.Create()
			if err != nil {
				return err
			}
		}
		return transform.Transform(dir, db, maxParallelDBQueries, maxParallelKVWrites, batchSize, !noPrivacy, structured)
	},
}

func transformCLI() *cobra.Command {
	transformCmd = addDataDir(transformCmd)
	transformCmd = addDatabase(transformCmd)
	transformCmd.Flags().IntVarP(
		&maxParallelDBQueries,
		"max-parallel-db-queries",
		"m",
		transform.MaxParallelDBQueries,
		"maximum parallel database queries",
	)
	transformCmd.Flags().IntVarP(
		&maxParallelKVWrites,
		"max-parallel-kv-writes",
		"k",
		transform.MaxParallelKVWrites,
		"the default is optimized for high throughput SATA SSD. Recommended values are between 64 and 128 for HDD, 256 and 1,024 for SSD, and 4,096 and 16,384 for NVMe SSD.",
	)
	transformCmd.Flags().IntVarP(&batchSize, "batch-size", "b", transform.BatchSize, "size of the batch to save to the database")
	transformCmd.Flags().BoolVarP(&cleanUp, "clean-up", "c", cleanUp, "drop & recreate the database table before starting")
	transformCmd.Flags().BoolVarP(&noPrivacy, "no-privacy", "p", noPrivacy, "include email addresses, CPF and other PII in the JSON data")
	transformCmd.Flags().BoolVarP(&structured, "structured", "", structured, "save data to structured tables (business and business_partners) instead of JSON table")
	return transformCmd
}

const importPartnersHelper = `
Import only partners data from Socios CSV files into socios_cnpj table.
This command reads Socios files and inserts partners directly into the database,
requiring that the business table already exists with the corresponding CNPJs.
`

var importPartnersCmd = &cobra.Command{
	Use:   "import-partners",
	Short: "Imports only partners data from Socios CSV files",
	Long:  importPartnersHelper,
	RunE: func(_ *cobra.Command, _ []string) error {
		if err := assertDirExists(); err != nil {
			return err
		}
		db, err := loadDatabase()
		if err != nil {
			return fmt.Errorf("could not find database: %w", err)
		}
		defer db.Close()
		return transform.TransformPartnersOnly(dir, db, maxParallelDBQueries)
	},
}

func importPartnersCLI() *cobra.Command {
	importPartnersCmd = addDataDir(importPartnersCmd)
	importPartnersCmd = addDatabase(importPartnersCmd)
	importPartnersCmd.Flags().IntVarP(
		&maxParallelDBQueries,
		"max-parallel-db-queries",
		"m",
		transform.MaxParallelDBQueries,
		"maximum parallel database queries",
	)
	return importPartnersCmd
}
