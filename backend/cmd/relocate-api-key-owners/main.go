package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	_ "github.com/lib/pq"
)

func main() {
	opts := runOptions{}
	flag.StringVar(&opts.ManifestPath, "manifest", "", "required frozen owner-relocation manifest path")
	flag.StringVar(&opts.ExclusionsPath, "exclusions", "", "optional approved exclusion-ID JSON path; forbidden for exact subset manifests")
	flag.StringVar(&opts.ExpectedChecksum, "manifest-sha256", "", "required full frozen manifest SHA-256 for exact subset dry-run and execute")
	flag.BoolVar(&opts.Execute, "execute", false, "execute one bounded relocation batch (default is full dry-run)")
	flag.IntVar(&opts.BatchSize, "batch-size", maxBatchSize, "execute batch size (1-25)")
	flag.Int64Var(&opts.AfterID, "after-id", 0, "select transferable remote IDs greater than this value")
	flag.Int64Var(&opts.OnlyID, "only-id", 0, "select exactly one transferable remote key ID (canary)")
	flag.StringVar(&opts.CorrelationID, "correlation-id", "", "required non-secret operation correlation ID in execute mode")
	flag.IntVar(&opts.BatchNumber, "batch-number", 0, "required positive batch number in execute mode")
	flag.StringVar(&opts.Direction, "direction", "forward", "forward or rollback")
	flag.BoolVar(&opts.AllowProtectedDrift, "allow-live-protected-drift", false, "allow manifest protected digest drift while still enforcing current DB pre/post invariants")
	flag.BoolVar(&opts.RepairCacheOnly, "repair-cache-only", false, "for already committed selected rows, retry auth-cache invalidation without changing database ownership")
	flag.Parse()

	if err := validateOptions(opts); err != nil {
		log.Fatal(err)
	}
	value, checksum, err := loadManifest(opts.ManifestPath)
	if err != nil {
		log.Fatal(err)
	}
	exclusions := make(map[int64]struct{})
	if strings.TrimSpace(opts.ExclusionsPath) != "" {
		exclusions, err = loadExclusions(opts.ExclusionsPath)
		if err != nil {
			log.Fatal(err)
		}
	}

	cfg, err := config.LoadForBootstrap()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	db, err := sql.Open("postgres", cfg.Database.DSNWithTimezone(cfg.Timezone))
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer func() { _ = db.Close() }()
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("database ping: %v", err)
	}

	var cache service.APIKeyCache
	if opts.Execute {
		redisClient := repository.InitRedis(cfg)
		defer func() { _ = redisClient.Close() }()
		if err := redisClient.Ping(ctx).Err(); err != nil {
			log.Fatalf("redis ping: %v", err)
		}
		cache = repository.NewAPIKeyCache(redisClient)
	}

	result, err := run(ctx, db, cache, value, checksum, exclusions, opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, formatSummary(result, opts.Execute, checksum, opts.Direction))
		log.Fatal(sanitizeCLIError(err))
	}
	fmt.Println(formatSummary(result, opts.Execute, checksum, opts.Direction))
}

func sanitizeCLIError(err error) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	for _, marker := range []string{"sk-", "Bearer ", "Authorization:"} {
		if strings.Contains(message, marker) {
			return fmt.Errorf("operation failed with a redacted sensitive error")
		}
	}
	return err
}
