package cmd

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"

	"github.com/cuducos/minha-receita/db"
	"github.com/spf13/cobra"
)

func exportCSVCLI() *cobra.Command {
	var outputFile string

	cmd := &cobra.Command{
		Use:   "export-csv",
		Short: "Exporta dados do banco para CSV",
		Long:  "Exporta empresas sem sócios para um arquivo CSV",
		RunE: func(cmd *cobra.Command, args []string) error {
			if outputFile == "" {
				outputFile = "empresas_sem_socios.csv"
			}

			database, err := loadDatabase()
			if err != nil {
				return fmt.Errorf("erro ao conectar ao banco: %w", err)
			}
			defer database.Close()

			pg, ok := database.(*db.PostgreSQL)
			if !ok {
				return fmt.Errorf("este comando só funciona com PostgreSQL")
			}

			// Obter schema e construir nomes completos das tabelas
			schema := pg.Schema()
			businessTable := fmt.Sprintf("%s.business", schema)
			sociosTable := fmt.Sprintf("%s.socios_cnpj", schema)

			// Query SQL fornecida pelo usuário
			query := fmt.Sprintf(`
				SELECT 
					b.id,
					b.cnpj,
					b.razao_social,
					b.telefones 
				FROM %s b
				LEFT JOIN %s s 
					ON s.business_id = b.id
				WHERE s.id IS NULL
			`, businessTable, sociosTable)

			ctx := context.Background()
			rows, err := pg.Pool().Query(ctx, query)
			if err != nil {
				return fmt.Errorf("erro ao executar query: %w", err)
			}
			defer rows.Close()

			// Criar arquivo CSV
			file, err := os.Create(outputFile)
			if err != nil {
				return fmt.Errorf("erro ao criar arquivo CSV: %w", err)
			}
			defer file.Close()

			writer := csv.NewWriter(file)
			defer writer.Flush()

			// Escrever cabeçalho
			header := []string{"id", "cnpj", "razao_social", "telefones"}
			if err := writer.Write(header); err != nil {
				return fmt.Errorf("erro ao escrever cabeçalho: %w", err)
			}

			// Escrever dados
			count := 0
			for rows.Next() {
				var id int64
				var cnpj, razaoSocial, telefones string

				if err := rows.Scan(&id, &cnpj, &razaoSocial, &telefones); err != nil {
					return fmt.Errorf("erro ao ler linha: %w", err)
				}

				record := []string{
					fmt.Sprintf("%d", id),
					cnpj,
					razaoSocial,
					telefones,
				}

				if err := writer.Write(record); err != nil {
					return fmt.Errorf("erro ao escrever linha: %w", err)
				}
				count++
			}

			if err := rows.Err(); err != nil {
				return fmt.Errorf("erro ao iterar resultados: %w", err)
			}

			fmt.Printf("CSV criado com sucesso: %s\n", outputFile)
			fmt.Printf("Total de registros exportados: %d\n", count)

			return nil
		},
	}

	cmd.Flags().StringVarP(&outputFile, "output", "o", "empresas_sem_socios.csv", "Nome do arquivo CSV de saída")
	return addDatabase(cmd)
}

