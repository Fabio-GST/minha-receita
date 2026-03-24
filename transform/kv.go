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

func (kv *badgerStorage) loadSource(ctx context.Context, s *source, l *lookups, bar *progressbar.ProgressBar, m int) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(m)
	ch := make(chan []string)
	g.Go(func() error {
		defer close(ch)
		err := s.sendTo(ctx, ch)
		if err == io.EOF {
			return nil
		}
		return err
	})
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case r, ok := <-ch:
				if !ok {
					return nil
				}
				g.Go(func() error {
					if err := kv.loadRow(r, s.kind, l); err != nil {
						return err
					}
					return bar.Add(1)
				})
			}

		}
	})
	return g.Wait()
}

func (kv *badgerStorage) load(dir string, l *lookups, m int) error {
	kinds := []sourceType{base, partners, simpleTaxes}
	for _, opt := range []sourceType{noTaxes, presumedProfit, realProfit, arbitratedProfit} {
		if ps, _ := pathsForSource(opt, dir); len(ps) > 0 {
			kinds = append(kinds, opt)
		} else {
			slog.Info(fmt.Sprintf("No files found for %s, skipping.", string(opt)))
		}
	}
	srcs, t, err := newSources(dir, kinds)
	if err != nil {
		return fmt.Errorf("could not load sources: %w", err)
	}
	tic := time.NewTicker(3 * time.Minute)
	defer tic.Stop()
	go func() {
		for range tic.C {
			kv.garbageCollect()
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
		g.Go(func() error {
			return kv.loadSource(ctx, src, l, bar, m)
		})
	}
	return g.Wait()
}

func (kv *badgerStorage) enrichCompany(c *Company) error {
	n := cnpj.Base(c.CNPJ)
	p, err := partnersOf(kv.db, n)
	if err != nil {
		return fmt.Errorf("error enriching company: %w", err)
	}
	c.QuadroSocietario = p
	v, err := baseOf(kv.db, n)
	if err != nil {
		return fmt.Errorf("error enriching company: %w", err)
	}
	c.CodigoPorte = v.CodigoPorte
	c.Porte = v.Porte
	c.RazaoSocial = v.RazaoSocial
	c.CodigoNaturezaJuridica = v.CodigoNaturezaJuridica
	c.NaturezaJuridica = v.NaturezaJuridica
	c.QualificacaoDoResponsavel = v.QualificacaoDoResponsavel
	c.CapitalSocial = v.CapitalSocial
	c.EnteFederativoResponsavel = v.EnteFederativoResponsavel
	st, err := simpleTaxesOf(kv.db, n)
	if err != nil {
		return fmt.Errorf("error enriching company: %w", err)
	}
	c.OpcaoPeloSimples = st.OpcaoPeloSimples
	c.DataOpcaoPeloSimples = st.DataOpcaoPeloSimples
	c.DataExclusaoDoSimples = st.DataExclusaoDoSimples
	c.OpcaoPeloMEI = st.OpcaoPeloMEI
	c.DataOpcaoPeloMEI = st.DataOpcaoPeloMEI
	c.DataExclusaoDoMEI = st.DataExclusaoDoMEI
	tr, err := taxRegimeOf(kv.db, c.CNPJ)
	if err != nil {
		return fmt.Errorf("error enriching company: %w", err)
	}
	c.RegimeTributario = tr
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

func applyPlatformBadgerOptions(opt badger.Options) badger.Options {
	// Badger v4 double-maps vlog files (actual mmap = 2 * ValueLogFileSize).
	// On Windows this easily exceeds available virtual address space, so we
	// use a small value; Badger will simply roll over to more vlog files.
	if runtime.GOOS == "windows" {
		opt = opt.WithValueLogFileSize(32 << 20) // mmap per file ≈ 64MB
	}
	// 32-bit processes have a small virtual address space; trim caches and
	// compactor pressure so export/transform can finish without OOM.
	if runtime.GOARCH == "386" {
		opt = opt.WithValueLogFileSize(16 << 20).
			WithMemTableSize(8 << 20).
			WithNumMemtables(1).
			WithBlockCacheSize(8 << 20).
			WithIndexCacheSize(8 << 20).
			WithNumGoroutines(2)
	}
	return opt
}

func newBadgerStorage(dir string, ro bool) (*badgerStorage, error) {
	opt := applyPlatformBadgerOptions(badger.DefaultOptions(dir))
	// Badger read-only mode is not supported on Windows
	if ro && runtime.GOOS != "windows" {
		opt = opt.WithReadOnly(ro)
	}
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
