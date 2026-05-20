// Package configpg implements config.Storage backed by PostgreSQL.
//
// Config data is stored as individual rows (section/key/value) so you can
// query remotes directly from SQL. When config encryption is active, an
// additional encrypted blob is stored in a separate table and used as the
// canonical source on Load().
//
// Use it by passing a PostgreSQL connection URL to the --config flag:
//
//	rclone --config postgres://user:pass@host:5432/rcloneconfig ls remote:
package configpg

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"

	// Register the pgx PostgreSQL driver with database/sql
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	rowsTable  = "rclone_config"
	blobTable  = "rclone_config_encrypted"
)

// Storage implements config.Storage for PostgreSQL.
//
// Two persistence modes:
//   - Unencrypted: individual rows in rclone_config. SetValue/DeleteKey
//     write directly to the DB. Save() is a no-op.
//   - Encrypted: an encrypted blob in rclone_config_encrypted is the
//     canonical source. Load() decrypts it into the in-memory cache,
//     and Save() re-encrypts on writes. Individual rows are NOT written
//     when encryption is active (the encrypted blob is the source of truth).
type Storage struct {
	mu       sync.RWMutex
	db       *sql.DB
	schema   string                       // optional schema from search_path
	sections map[string]map[string]string // in-memory config cache
}

var rowsTableStmt = fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		section TEXT NOT NULL,
		key TEXT NOT NULL,
		value TEXT NOT NULL DEFAULT '',
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		PRIMARY KEY (section, key)
	)
`, rowsTable)

var blobTableStmt = fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
		data TEXT NOT NULL DEFAULT '',
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)
`, blobTable)

// Install connects to PostgreSQL and installs it as the active config storage.
func Install(ctx context.Context, connString string) error {
	s, err := NewStorage(ctx, connString)
	if err != nil {
		return err
	}
	config.SetStorage(s)
	return nil
}

// InstallWithDB installs an existing *sql.DB connection as the active
// config storage. The caller remains responsible for closing the db.
// schema is the (optional) PostgreSQL schema name, or "" for the default.
func InstallWithDB(ctx context.Context, db *sql.DB, schema string) error {
	s, err := newStorageWithDB(ctx, db, schema)
	if err != nil {
		return err
	}
	config.SetStorage(s)
	return nil
}

// NewStorage creates a config storage backed by PostgreSQL but does NOT
// install it as the global storage.
func NewStorage(ctx context.Context, connString string) (*Storage, error) {
	db, err := openPG(connString)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}
	s, err := newStorageWithDB(ctx, db, parseSearchPath(connString))
	if err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// NewStorageWithDB wraps an existing *sql.DB as a config storage without
// installing it globally. The caller remains responsible for closing db.
func NewStorageWithDB(ctx context.Context, db *sql.DB, schema string) (*Storage, error) {
	return newStorageWithDB(ctx, db, schema)
}

// newStorageWithDB is the shared constructor for Storage given an existing db.
func newStorageWithDB(ctx context.Context, db *sql.DB, schema string) (*Storage, error) {
	s := &Storage{
		db:       db,
		schema:   schema,
		sections: map[string]map[string]string{},
	}
	if err := s.migrate(ctx); err != nil {
		return nil, fmt.Errorf("failed to run postgres config migration: %w", err)
	}
	return s, nil
}

// migrate creates the schema (if specified) and both tables.
func (s *Storage) migrate(ctx context.Context) error {
	if s.schema != "" {
		if _, err := s.db.ExecContext(ctx,
			fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, quoteIdent(s.schema))); err != nil {
			return fmt.Errorf("failed to create schema %q: %w", s.schema, err)
		}
	}
	for _, stmt := range []string{rowsTableStmt, blobTableStmt} {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}
	return nil
}

// hasEncryptedBlob returns true if the encrypted blob row exists.
func (s *Storage) hasEncryptedBlob(ctx context.Context) bool {
	var count int
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT COUNT(*) FROM %s WHERE id = 1`, s.q(blobTable))).Scan(&count)
	return err == nil && count > 0
}

// isEncrypted returns true if a config encryption password is set.
func isEncrypted() bool {
	return config.IsEncrypted()
}

// resolveLoad orders the two data sources:
//  1. If rclone_config_encrypted has a row → decrypt blob, parse INI
//  2. Otherwise → read from rclone_config individual rows
func (s *Storage) resolveLoad(ctx context.Context) error {
	if s.hasEncryptedBlob(ctx) {
		return s.loadEncryptedBlob(ctx)
	}
	return s.loadRows(ctx)
}

// loadEncryptedBlob reads and decrypts the blob from rclone_config_encrypted.
func (s *Storage) loadEncryptedBlob(ctx context.Context) error {
	var data string
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT data FROM %s WHERE id = 1`, s.q(blobTable))).Scan(&data)
	if err != nil {
		if err == sql.ErrNoRows {
			s.sections = map[string]map[string]string{}
			return nil
		}
		return err
	}
	if data == "" {
		s.sections = map[string]map[string]string{}
		return nil
	}

	reader, err := config.DecryptConfigData(data)
	if err != nil {
		return fmt.Errorf("failed to decrypt config: %w", err)
	}
	decrypted, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read decrypted config: %w", err)
	}
	if err := json.Unmarshal(decrypted, &s.sections); err != nil {
		return fmt.Errorf("failed to parse config: %w", err)
	}
	return nil
}

// loadRows reads all entries from the individual-rows table.
func (s *Storage) loadRows(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(
		`SELECT section, key, value FROM %s ORDER BY section, key`, s.q(rowsTable)))
	if err != nil {
		return err
	}
	defer rows.Close()

	entries := map[string]map[string]string{}
	for rows.Next() {
		var section, key, value string
		if err := rows.Scan(&section, &key, &value); err != nil {
			fs.Errorf(nil, "configpg: loadRows scan failed: %v", err)
			continue
		}
		if entries[section] == nil {
			entries[section] = map[string]string{}
		}
		entries[section][key] = value
	}
	s.sections = entries
	return rows.Err()
}

// flushRows writes all in-memory sections to the individual-rows table.
// Used when encryption is being removed (transitioning back to plaintext).
func (s *Storage) flushRows(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s`, s.q(rowsTable))); err != nil {
		return err
	}

	for section, keys := range s.sections {
		for key, value := range keys {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
				INSERT INTO %s (section, key, value, updated_at)
				VALUES ($1, $2, $3, NOW())
				ON CONFLICT (section, key)
				DO UPDATE SET value = $3, updated_at = NOW()
			`, s.q(rowsTable)), section, key, value); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// saveBlob serializes the in-memory sections into an encrypted blob and
// stores it in rclone_config_encrypted. Individual rows are cleared since
// the encrypted blob is the canonical source when encryption is active.
func (s *Storage) saveBlob(ctx context.Context) error {
	plain, err := json.Marshal(s.sections)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	var buf bytes.Buffer
	if err := config.Encrypt(bytes.NewReader(plain), &buf); err != nil {
		return fmt.Errorf("failed to encrypt config: %w", err)
	}
	_, err = s.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s`, s.q(rowsTable)))
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, data, updated_at) VALUES (1, $1, NOW())
		ON CONFLICT (id) DO UPDATE SET data = $1, updated_at = NOW()
	`, s.q(blobTable)), buf.String())
	return err
}

// removeBlob deletes the encrypted blob row. Called by Save() when
// encryption is being removed.
func (s *Storage) removeBlob(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE id = 1`, s.q(blobTable)))
	return err
}

// quoteIdent quotes a PostgreSQL identifier safely.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// parseSearchPath extracts the first schema from search_path query parameter.
func parseSearchPath(connString string) string {
	u, err := url.Parse(connString)
	if err != nil {
		return ""
	}
	sp := u.Query().Get("search_path")
	if sp == "" {
		return ""
	}
	schemas := strings.Split(sp, ",")
	return strings.TrimSpace(schemas[0])
}

// q returns a qualified table name, with schema prefix if set.
func (s *Storage) q(table string) string {
	if s.schema != "" {
		return quoteIdent(s.schema) + "." + table
	}
	return table
}

// ---------------------------------------------------------------------------
// config.Storage interface
// ---------------------------------------------------------------------------

// GetSectionList returns all section names.
func (s *Storage) GetSectionList() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.sections))
	for sec := range s.sections {
		out = append(out, sec)
	}
	return out
}

// HasSection returns true if the section exists.
func (s *Storage) HasSection(section string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.sections[section]
	return ok
}

// DeleteSection removes an entire section and all its keys.
func (s *Storage) DeleteSection(section string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sections, section)
}

// GetKeyList returns all keys in a section.
func (s *Storage) GetKeyList(section string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sec := s.sections[section]
	out := make([]string, 0, len(sec))
	for k := range sec {
		out = append(out, k)
	}
	return out
}

// GetValue returns the value for a key in a section.
func (s *Storage) GetValue(section string, key string) (value string, found bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sec, ok := s.sections[section]
	if !ok {
		return "", false
	}
	val, ok := sec[key]
	return val, ok
}

// SetValue sets a key in a section.
func (s *Storage) SetValue(section string, key string, value string) {
	if strings.HasPrefix(section, ":") {
		fs.Logf(nil, "Can't save config %q for on the fly backend %q", key, section)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	sec, ok := s.sections[section]
	if !ok {
		sec = map[string]string{}
		s.sections[section] = sec
	}
	sec[key] = value
}

// DeleteKey removes a single key from a section.
func (s *Storage) DeleteKey(section string, key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	sec, ok := s.sections[section]
	if !ok {
		return false
	}
	_, ok = sec[key]
	if !ok {
		return false
	}
	delete(sec, key)
	if len(sec) == 0 {
		delete(s.sections, section)
	}
	return true
}

// Load reads config from PostgreSQL.
//   - If an encrypted blob exists, decrypts it and populates the in-memory cache.
//   - Otherwise, reads from individual rows.
func (s *Storage) Load() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resolveLoad(context.Background())
}

// Save persists the in-memory config to PostgreSQL.
//   - When encryption is active: writes the encrypted blob. Individual
//     rows are NOT updated (the blob is the canonical source).
//   - When encryption is NOT active: writes all individual rows and
//     removes any stale encrypted blob (e.g. after encryption removal).
func (s *Storage) Save() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx := context.Background()

	if isEncrypted() {
		return s.saveBlob(ctx)
	}

	if err := s.flushRows(ctx); err != nil {
		return fmt.Errorf("failed to write config rows: %w", err)
	}
	if err := s.removeBlob(ctx); err != nil {
		fs.Debugf(nil, "configpg: no encrypted blob to remove")
	}
	return nil
}

// Serialize dumps the config as JSON.
func (s *Storage) Serialize() (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	j, err := json.Marshal(s.sections)
	return string(j), err
}

// Close shuts down the database connection pool.
func (s *Storage) Close() error {
	return s.db.Close()
}

// MigrateFromFile reads an rclone config file and imports all entries into
// PostgreSQL. Handles encrypted and plain files. Uses Save() to persist,
// so encryption state of the destination is determined by whether a config
// password is active in the current session.
func MigrateFromFile(ctx context.Context, connString string, filePath string) error {
	entries, err := config.LoadFile(filePath)
	if err != nil {
		return err
	}

	s, err := NewStorage(ctx, connString)
	if err != nil {
		return err
	}
	defer s.Close()

	s.mu.Lock()
	s.sections = entries
	s.mu.Unlock()

	if err := s.Save(); err != nil {
		return fmt.Errorf("failed to save migrated config: %w", err)
	}
	return nil
}

// openPG opens a PostgreSQL connection pool with sensible defaults.
func openPG(connString string) (*sql.DB, error) {
	db, err := sql.Open("pgx", connString)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetConnMaxIdleTime(5 * time.Minute)
	return db, nil
}

// Check the interface is satisfied
var _ config.Storage = (*Storage)(nil)
