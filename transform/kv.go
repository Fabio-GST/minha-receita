package transform

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/cuducos/go-cnpj"
	"github.com/dgraph-io/badger/v4"
	"github.com/schollz/progressbar/v3"
	"golang.org/x/sync/errgroup"
)

type item struct {
	key, value []byte
	kind       sourceType
}

func checksumFor(r []string) string {
	b := []byte(strings.Join(r, ""))
	h := md5.New()
	return hex.EncodeToString(h.Sum(b))
}

func newKVItem(s sourceType, l *lookups, r []string) (i item, err error) {
	var k string
	var h func(l *lookups, r []string) ([]byte, error)
	switch s {
	case partners:
		k = keyForPartners(r[0])
		h = loadPartnerRow
	case base:
		k = keyForBase(r[0])
		h = loadBaseRow
	case simpleTaxes:
		k = keyForSimpleTaxes(r[0])
		h = loadSimpleTaxesRow
	case realProfit:
		k = keyForTaxRegime(r[1])
		h = loadTaxRow
	case presumedProfit:
		k = keyForTaxRegime(r[1])
		h = loadTaxRow
	case arbitratedProfit:
		k = keyForTaxRegime(r[1])
		h = loadTaxRow
	case noTaxes:
		k = keyForTaxRegime(r[1])
		h = loadTaxRow
	default:
		return item{}, fmt.Errorf("unknown source type %s", string(s))
	}
	if s.isAccumulative() {
		k = k + ":" + checksumFor(r)
	}
	i.key = []byte(k)
	i.value, err = h(l, r)
	if err != nil {
		return item{}, fmt.Errorf("error loading value from source: %w", err)
	}
	i.kind = s
	return i, nil
}

type badgerStorage struct {
	db   *badger.DB
	path string
}

func (kv *badgerStorage) garbageCollect() {
	for {
		err := kv.db.RunValueLogGC(0.5)
		if err == badger.ErrRejected { // db already closed or more than one gc running
			return
		}
		if err == badger.ErrNoRewrite { // no garbage to collect
			return
		}
		if err != nil {
			slog.Error("Error running garbage collection", "error", err)
			return
		}
	}
}

// Tamanho do chunk para salvar múltiplos itens em uma única transação
const kvChunkSize = 1000

func (kv *badgerStorage) loadRow(r []string, s sourceType, l *lookups) error {
	i, err := newKVItem(s, l, r)
	if err != nil {
		return fmt.Errorf("error creating an %s item: %w", s, err)
	}
	if err := kv.db.Update(func(tx *badger.Txn) error { return tx.Set(i.key, i.value) }); err != nil {
		return fmt.Errorf("could not save key-value: %w", err)
	}
	return nil
}

// loadChunk salva múltiplos itens em uma única transação para melhor performance
func (kv *badgerStorage) loadChunk(items []item) error {
	return kv.db.Update(func(tx *badger.Txn) error {
		for _, i := range items {
			if err := tx.Set(i.key, i.value); err != nil {
				return fmt.Errorf("could not set key-value in chunk: %w", err)
			}
		}
		return nil
	})
}

func (kv *badgerStorage) loadSource(ctx context.Context, s *source, l *lookups, bar *progressbar.ProgressBar, m int) error {
	g, ctx := errgroup.WithContext(ctx)
	// Buffer canal para evitar bloqueios e acúmulo de memória
	// Buffer de 1000 linhas reduz bloqueios sem usar muita memória
	ch := make(chan []string, 1000)
	g.Go(func() error {
		defer close(ch)
		err := s.sendTo(ctx, ch)
		if err == io.EOF {
			return nil
		}
		return err
	})
	
	// Canal para processar chunks - buffer suficiente para múltiplos chunks
	chunkCh := make(chan []item, m*2)
	
	// Múltiplos workers para processar chunks em paralelo
	// Limitar número de workers para evitar sobrecarga
	numWorkers := m
	if numWorkers > 8 {
		numWorkers = 8 // Máximo de 8 workers para chunks
	}
	
	// Workers para processar chunks
	for w := 0; w < numWorkers; w++ {
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return nil
				case chunk, ok := <-chunkCh:
					if !ok {
						return nil
					}
					if err := kv.loadChunk(chunk); err != nil {
						return fmt.Errorf("error loading chunk: %w", err)
					}
					// Atualizar progresso para todos os itens do chunk
					if err := bar.Add(len(chunk)); err != nil {
						return err
					}
				}
			}
		})
	}
	
	// Acumular linhas em chunks
	g.Go(func() error {
		defer close(chunkCh)
		chunk := make([]item, 0, kvChunkSize)
		
		for {
			select {
			case <-ctx.Done():
				// Salvar chunk pendente antes de sair
				if len(chunk) > 0 {
					select {
					case chunkCh <- chunk:
					case <-ctx.Done():
						return nil
					}
				}
				return nil
			case r, ok := <-ch:
				if !ok {
					// Canal fechado, salvar chunk pendente
					if len(chunk) > 0 {
						select {
						case chunkCh <- chunk:
						case <-ctx.Done():
							return nil
						}
					}
					return nil
				}
				
				// Criar item
				i, err := newKVItem(s.kind, l, r)
				if err != nil {
					return fmt.Errorf("error creating item: %w", err)
				}
				
				chunk = append(chunk, i)
				
				// Quando chunk atingir o tamanho, enviar para processamento
				if len(chunk) >= kvChunkSize {
					select {
					case chunkCh <- chunk:
						chunk = make([]item, 0, kvChunkSize) // Novo chunk vazio
					case <-ctx.Done():
						return nil
					}
				}
			}
		}
	})
	
	return g.Wait()
}

func (kv *badgerStorage) load(dir string, l *lookups, m int) error {
	srcs, t, err := newSources(dir, []sourceType{
		base,
		partners,
		simpleTaxes,
		noTaxes,
		presumedProfit,
		realProfit,
		arbitratedProfit,
	})
	if err != nil {
		return fmt.Errorf("could not load sources: %w", err)
	}
	// GC mais frequente para reduzir uso de memória (a cada 1 minuto)
	tic := time.NewTicker(1 * time.Minute)
	defer tic.Stop()
	go func() {
		for range tic.C {
			kv.garbageCollect()
			// Forçar garbage collection do Go runtime também
			runtime.GC()
		}
	}()
	bar := progressbar.Default(t, "Processing base CNPJ, partners and taxes")
	defer func() {
		if err := bar.Close(); err != nil {
			slog.Warn("could not close the progress bar", "error", err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)
	for _, src := range srcs {
		src := src // Capturar variável para closure
		g.Go(func() error {
			return kv.loadSource(ctx, src, l, bar, m)
		})
	}
	return g.Wait()
}

func (kv *badgerStorage) enrichCompany(c *Company) error {
	n := cnpj.Base(c.CNPJ)
	ps := make(chan []PartnerData)
	bs := make(chan baseData)
	st := make(chan simpleTaxesData)
	tr := make(chan TaxRegimes)
	errs := make(chan error)
	go func() {
		p, err := partnersOf(kv.db, n)
		if err != nil {
			errs <- err
		}
		ps <- p
	}()
	go func() {
		v, err := baseOf(kv.db, n)
		if err != nil {
			errs <- err
		}
		bs <- v
	}()
	go func() {
		t, err := simpleTaxesOf(kv.db, n)
		if err != nil {
			errs <- err
		}
		st <- t
	}()
	go func() {
		t, err := taxRegimeOf(kv.db, c.CNPJ)
		if err != nil {
			errs <- err
		}
		tr <- t
	}()
	for range [4]int{} {
		select {
		case p := <-ps:
			c.QuadroSocietario = p
		case v := <-bs:
			c.CodigoPorte = v.CodigoPorte
			c.Porte = v.Porte
			c.RazaoSocial = v.RazaoSocial
			c.CodigoNaturezaJuridica = v.CodigoNaturezaJuridica
			c.NaturezaJuridica = v.NaturezaJuridica
			c.QualificacaoDoResponsavel = v.QualificacaoDoResponsavel
			c.CapitalSocial = v.CapitalSocial
			c.EnteFederativoResponsavel = v.EnteFederativoResponsavel
		case t := <-st:
			c.OpcaoPeloSimples = t.OpcaoPeloSimples
			c.DataOpcaoPeloSimples = t.DataOpcaoPeloSimples
			c.DataExclusaoDoSimples = t.DataExclusaoDoSimples
			c.OpcaoPeloMEI = t.OpcaoPeloMEI
			c.DataOpcaoPeloMEI = t.DataOpcaoPeloMEI
			c.DataExclusaoDoMEI = t.DataExclusaoDoMEI
		case t := <-tr:
			c.RegimeTributario = t
		case err := <-errs:
			return fmt.Errorf("error enriching company: %w", err)
		}
	}
	return nil
}

func (b *badgerStorage) close() error {
	return b.db.Close()
}

type noLogger struct{}

func (*noLogger) Errorf(string, ...any)   {}
func (*noLogger) Warningf(string, ...any) {}
func (*noLogger) Infof(string, ...any)    {}
func (*noLogger) Debugf(string, ...any)   {}

func newBadgerStorage(dir string, ro bool) (*badgerStorage, error) {
	opt := badger.DefaultOptions(dir)
	// Badger read-only mode is not supported on Windows
	if ro && runtime.GOOS != "windows" {
		opt = opt.WithReadOnly(ro)
	}
	// Optimize memory usage for large datasets
	opt = opt.WithNumMemtables(1)           // Reduce from default 5 to 1
	opt = opt.WithNumLevelZeroTables(1)     // Reduce from default 5 to 1
	opt = opt.WithNumLevelZeroTablesStall(2) // Reduce from default 10 to 2
	opt = opt.WithValueLogMaxEntries(100000) // Limit value log entries
	// Reduzir tamanho do value log para economizar memória e disco
	opt = opt.WithValueLogFileSize(64 << 20) // 64MB (padrão é 1GB)
	// Reduzir tamanho das memtables
	opt = opt.WithMemTableSize(16 << 20) // 16MB (padrão é 64MB)
	slog.Debug("Creating temporary key-value storage", "path", dir)
	if os.Getenv("DEBUG") == "" {
		opt = opt.WithLogger(&noLogger{})
	}
	db, err := badger.Open(opt)
	if err != nil {
		return nil, fmt.Errorf("error creating badger key-value object: %w", err)
	}
	return &badgerStorage{db: db, path: dir}, nil
}
