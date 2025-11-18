package transform

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cuducos/minha-receita/download"
	"github.com/schollz/progressbar/v3"
)

const (
	// MaxParallelDBQueries is the default for maximum number of parallels save
	// queries sent to the database
	MaxParallelDBQueries = 4

	// MaxParallelKVWrites is the default for maximum number of parallels
	// writes on the key-value storage (Badger)
	MaxParallelKVWrites = 256

	// BatchSize determines the size of the batches used to create the initial JSON
	// data in the database.
	// Reduced from 2048 to 512 to use less disk space during transactions
	BatchSize = 512
)

var extraIdexes = [...]string{
	"cnae_fiscal",
	"cnaes_secundarios.codigo",
	"codigo_municipio",
	"codigo_municipio_ibge",
	"codigo_natureza_juridica",
	"qsa.cnpj_cpf_do_socio",
	"uf",
}

type database interface {
	PreLoad() error
	CreateCompanies([][]string) error
	CreateCompaniesStructured([][]string) error
	CreateCompaniesStructuredDirect([]Company) error // Optimized version that doesn't require JSON
	PostLoad() error
	CreateExtraIndexes([]string) error
	MetaSave(string, string) error
}

type partnersDatabase interface {
	ImportPartnersOnly([]PartnerData, string) error
	ImportPartnersBatch(map[string][]PartnerData) error
	MetaSave(string, string) error
}

type kvStorage interface {
	load(string, *lookups, int) error
	enrichCompany(*Company) error
	close() error
}

func saveUpdatedAt(db database, dir string) error {
	slog.Info("Saving the updated at date to the database…")
	p := filepath.Join(dir, download.FederalRevenueUpdatedAt)
	v, err := os.ReadFile(p)
	if err != nil {
		return fmt.Errorf("error reading %s: %w", p, err)

	}
	return db.MetaSave("updated-at", string(v))
}

func saveUpdatedAtPartners(db partnersDatabase, dir string) error {
	slog.Info("Saving the updated at date to the database…")
	p := filepath.Join(dir, download.FederalRevenueUpdatedAt)
	v, err := os.ReadFile(p)
	if err != nil {
		return fmt.Errorf("error reading %s: %w", p, err)

	}
	return db.MetaSave("updated-at", string(v))
}

func createKeyValueStorage(dir string, pth string, l lookups, maxKV int) (err error) { // using named return so we can set it in the defer call
	kv, err := newBadgerStorage(pth, false)
	if err != nil {
		return fmt.Errorf("could not create badger storage: %w", err)
	}
	defer func() {
		if e := kv.close(); e != nil && err == nil {
			err = fmt.Errorf("could not close key/value storage: %w", e)
		}
	}()
	if err := kv.load(dir, &l, maxKV); err != nil {
		return fmt.Errorf("error loading data to badger: %w", err)
	}
	return nil
}

func createJSONs(dir string, pth string, db database, l lookups, maxDB, batchSize int, privacy bool, structured bool) error {
	kv, err := newBadgerStorage(pth, true)
	if err != nil {
		return fmt.Errorf("could not create badger storage: %w", err)
	}
	defer func() {
		if err := kv.close(); err != nil {
			slog.Warn("could not close key-value storage", "path", pth, "error", err)
		}
	}()
	j, err := createJSONRecordsTask(dir, db, &l, kv, batchSize, privacy, structured)
	if err != nil {
		return fmt.Errorf("error creating new task for venues in %s: %w", dir, err)
	}
	if err := j.run(maxDB); err != nil {
		return fmt.Errorf("error writing venues to database: %w", err)
	}
	return saveUpdatedAt(db, dir)
}

func postLoad(db database) error {
	slog.Info("Consolidating the database…")
	if err := db.PostLoad(); err != nil {
		return err
	}
	slog.Info("Database consolidated!")
	slog.Info("Creating indexes…")
	if err := db.CreateExtraIndexes(extraIdexes[:]); err != nil {
		return err
	}
	slog.Info("Indexes created!")
	return nil
}

// Transform the downloaded files for company venues creating a database record
// per CNPJ
func Transform(dir string, db database, maxDB, maxKV, s int, p bool, structured bool) error {
	pth, err := os.MkdirTemp("", fmt.Sprintf("minha-receita-%s-*", time.Now().Format("20060102150405")))
	if err != nil {
		return fmt.Errorf("error creating temporary key-value storage: %w", err)
	}
	// Always clean up temporary directory, even on panic
	defer func() {
		slog.Info("Cleaning up temporary directory", "path", pth)
		if err := os.RemoveAll(pth); err != nil {
			slog.Error("could not remove temporary directory", "directory", pth, "error", err)
			slog.Warn("Temporary directory not removed - you may need to clean it manually", "path", pth)
		} else {
			slog.Info("Temporary directory removed successfully", "path", pth)
		}
	}()
	l, err := newLookups(dir)
	if err != nil {
		return fmt.Errorf("error creating look up tables from %s: %w", dir, err)
	}
	if err := createKeyValueStorage(dir, pth, l, maxKV); err != nil {
		return err
	}
	if err := createJSONs(dir, pth, db, l, maxDB, s, p, structured); err != nil {
		return err
	}
	return postLoad(db)
}

// TransformPartnersOnly imports only partners data from Socios CSV files
// This function reads Socios files and inserts partners directly into socios_cnpj table
// Optimized for performance with parallel processing and progress tracking
func TransformPartnersOnly(dir string, db partnersDatabase, maxDB int) error {
	slog.Info("Starting partners-only import...")
	
	// Load lookups (needed for partner parsing)
	l, err := newLookups(dir)
	if err != nil {
		return fmt.Errorf("error creating look up tables from %s: %w", dir, err)
	}
	
	// Load only partners source
	partnersSource, err := newSource(context.Background(), partners, dir)
	if err != nil {
		return fmt.Errorf("error loading partners source: %w", err)
	}
	defer partnersSource.close()
	
	totalRecords := partnersSource.total
	slog.Info(fmt.Sprintf("Found %d partner records to process", totalRecords))
	
	// Create progress bar
	bar := progressbar.Default(totalRecords, "Processing partners")
	defer func() {
		if err := bar.Close(); err != nil {
			slog.Warn("could not close progress bar", "error", err)
		}
	}()
	
	// Process partners in optimized batches
	batch := make(map[string][]PartnerData) // CNPJ -> []PartnerData
	batchSize := 5000 // Number of partners (not CNPJs) per batch
	currentBatchCount := 0 // Track total partners in current batch
	processed := int64(0)
	processedMutex := &sync.Mutex{}
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	ch := make(chan []string, 1000) // Larger buffer for better throughput
	errCh := make(chan error, 1)
	
	// Start reading partners
	go func() {
		defer close(ch)
		if err := partnersSource.sendTo(ctx, ch); err != nil {
			select {
			case errCh <- fmt.Errorf("error reading partners: %w", err):
			default:
			}
		}
	}()
	
	// Worker pool for parallel processing
	workers := maxDB
	if workers < 1 {
		workers = 1
	}
	if workers > 10 {
		workers = 10 // Cap at 10 workers to avoid overwhelming the database
	}
	
	batchCh := make(chan map[string][]PartnerData, workers*2)
	var wg sync.WaitGroup
	
	// Start worker goroutines
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range batchCh {
				if err := processPartnersBatchOptimized(db, batch, bar, &processed, processedMutex); err != nil {
					select {
					case errCh <- fmt.Errorf("error processing batch: %w", err):
					default:
					}
					return
				}
			}
		}()
	}
	
	// Process partners and send batches to workers
	go func() {
		defer close(batchCh)
		for {
			select {
			case <-errCh:
				cancel()
				return
			case row, ok := <-ch:
				if !ok {
					// Channel closed, process remaining batch
					if len(batch) > 0 {
						select {
						case batchCh <- batch:
						case <-ctx.Done():
							return
						}
					}
					return
				}
				
				// Parse partner row
				if len(row) < 11 {
					bar.Add(1)
					continue
				}
				
				partner, err := newPartnerData(&l, row)
				if err != nil {
					slog.Warn("error parsing partner data", "error", err)
					bar.Add(1)
					continue
				}
				
				cnpj := row[0] // CNPJ is first column in Socios file
				batch[cnpj] = append(batch[cnpj], partner)
				currentBatchCount++
				
				// Send batch to workers when it reaches batchSize (total partners, not CNPJs)
				if currentBatchCount >= batchSize {
					select {
					case batchCh <- batch:
						batch = make(map[string][]PartnerData)
						currentBatchCount = 0
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	
	// Wait for workers to finish
	wg.Wait()
	
	// Check for errors
	select {
	case err := <-errCh:
		return err
	default:
	}
	
	processedMutex.Lock()
	finalProcessed := processed
	processedMutex.Unlock()
	
	slog.Info(fmt.Sprintf("Successfully processed %d partner records", finalProcessed))
	
	// Save updated-at metadata if db supports it
	if err := saveUpdatedAtPartners(db, dir); err != nil {
		slog.Warn("could not save updated-at metadata", "error", err)
	}
	
	return nil
}

func processPartnersBatchOptimized(db partnersDatabase, batch map[string][]PartnerData, bar *progressbar.ProgressBar, processed *int64, mutex *sync.Mutex) error {
	// Process all CNPJs in a single batch query for better performance
	if err := db.ImportPartnersBatch(batch); err != nil {
		return fmt.Errorf("error importing partners batch: %w", err)
	}
	
	// Update progress for all partners in batch
	totalPartners := 0
	for _, partners := range batch {
		totalPartners += len(partners)
	}
	
	mutex.Lock()
	*processed += int64(totalPartners)
	currentProcessed := *processed
	mutex.Unlock()
	
	bar.Add(totalPartners)
	
	// Log progress every 10000 records
	if currentProcessed%10000 == 0 {
		slog.Info(fmt.Sprintf("Processed %d partner records...", currentProcessed))
	}
	
	return nil
}
