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

	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	defaultConfigName = "main"
	rowsTable         = "rclone_config"
	blobTable         = "rclone_config_encrypted"
)

// Storage implements config.Storage for PostgreSQL.
//
// Multiple named configs can live in the same schema — each Storage picks
// one by configName (parsed from the "config_name" query param, or "main").
//
// Two persistence modes:
//   - Unencrypted: individual rows in rclone_config, keyed by config_name.
//   - Encrypted: an encrypted blob in rclone_config_encrypted is the
//     canonical source; individual rows are cleared.
type Storage struct {
	mu         sync.RWMutex
	db         *sql.DB
	schema     string
	configName string
	sections   map[string]map[string]string
}

func rowsTableStmt(q func(string) string) string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			config_name TEXT NOT NULL DEFAULT 'main',
			section TEXT NOT NULL,
			key TEXT NOT NULL,
			value TEXT NOT NULL DEFAULT '',
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (config_name, section, key)
		)
	`, q(rowsTable))
}

func blobTableStmt(q func(string) string) string {
	return fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			config_name TEXT NOT NULL DEFAULT 'main',
			id INTEGER NOT NULL DEFAULT 1,
			data TEXT NOT NULL DEFAULT '',
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			PRIMARY KEY (config_name, id)
		)
	`, q(blobTable))
}

func Install(ctx context.Context, connString string) error {
	s, err := NewStorage(ctx, connString)
	if err != nil {
		return err
	}
	config.SetStorage(s)
	return nil
}

func InstallWithDB(ctx context.Context, db *sql.DB, schema, configName string) error {
	s, err := newStorageWithDB(ctx, db, schema, configName)
	if err != nil {
		return err
	}
	config.SetStorage(s)
	return nil
}

func NewStorage(ctx context.Context, connString string) (*Storage, error) {
	pgConn := stripConfigName(connString)
	db, err := openPG(pgConn)
	if err != nil {
		return nil, err
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}
	s, err := newStorageWithDB(ctx, db, parseSearchPath(connString), parseConfigName(connString))
	if err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func NewStorageWithDB(ctx context.Context, db *sql.DB, schema, configName string) (*Storage, error) {
	return newStorageWithDB(ctx, db, schema, configName)
}

func newStorageWithDB(ctx context.Context, db *sql.DB, schema, configName string) (*Storage, error) {
	s := &Storage{
		db:         db,
		schema:     schema,
		configName: configName,
		sections:   map[string]map[string]string{},
	}
	if err := s.migrate(ctx); err != nil {
		return nil, fmt.Errorf("failed to run postgres config migration: %w", err)
	}
	return s, nil
}

func (s *Storage) migrate(ctx context.Context) error {
	if s.schema != "" {
		if _, err := s.db.ExecContext(ctx,
			fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, quoteIdent(s.schema))); err != nil {
			return fmt.Errorf("failed to create schema %q: %w", s.schema, err)
		}
	}
	for _, stmt := range []func(func(string) string) string{rowsTableStmt, blobTableStmt} {
		if _, err := s.db.ExecContext(ctx, stmt(s.q)); err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
	}
	return nil
}

func (s *Storage) hasEncryptedBlob(ctx context.Context) bool {
	var count int
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT COUNT(*) FROM %s WHERE config_name = $1 AND id = 1`,
		s.q(blobTable)), s.configName).Scan(&count)
	return err == nil && count > 0
}

func isEncrypted() bool {
	return config.IsEncrypted()
}

func (s *Storage) resolveLoad(ctx context.Context) error {
	if s.hasEncryptedBlob(ctx) {
		return s.loadEncryptedBlob(ctx)
	}
	return s.loadRows(ctx)
}

func (s *Storage) loadEncryptedBlob(ctx context.Context) error {
	var data string
	err := s.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT data FROM %s WHERE config_name = $1 AND id = 1`,
		s.q(blobTable)), s.configName).Scan(&data)
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

func (s *Storage) loadRows(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf(
		`SELECT section, key, value FROM %s WHERE config_name = $1 ORDER BY section, key`,
		s.q(rowsTable)), s.configName)
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

func (s *Storage) flushRows(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, fmt.Sprintf(
		`DELETE FROM %s WHERE config_name = $1`, s.q(rowsTable)), s.configName); err != nil {
		return err
	}

	for section, keys := range s.sections {
		for key, value := range keys {
			if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
				INSERT INTO %s (config_name, section, key, value, updated_at)
				VALUES ($1, $2, $3, $4, NOW())
				ON CONFLICT (config_name, section, key)
				DO UPDATE SET value = $4, updated_at = NOW()
			`, s.q(rowsTable)), s.configName, section, key, value); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (s *Storage) saveBlob(ctx context.Context) error {
	plain, err := json.Marshal(s.sections)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}
	var buf bytes.Buffer
	if err := config.Encrypt(bytes.NewReader(plain), &buf); err != nil {
		return fmt.Errorf("failed to encrypt config: %w", err)
	}

	_, err = s.db.ExecContext(ctx, fmt.Sprintf(
		`DELETE FROM %s WHERE config_name = $1`, s.q(rowsTable)), s.configName)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (config_name, id, data, updated_at) VALUES ($1, 1, $2, NOW())
		ON CONFLICT (config_name, id) DO UPDATE SET data = $2, updated_at = NOW()
	`, s.q(blobTable)), s.configName, buf.String())
	return err
}

func (s *Storage) removeBlob(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(
		`DELETE FROM %s WHERE config_name = $1 AND id = 1`,
		s.q(blobTable)), s.configName)
	return err
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

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

func parseConfigName(connString string) string {
	u, err := url.Parse(connString)
	if err != nil {
		return defaultConfigName
	}
	if v := u.Query().Get("config_name"); v != "" {
		return v
	}
	return defaultConfigName
}

// stripConfigName removes the config_name query parameter from a PG
// connection string so it isn't forwarded as a server parameter.
func stripConfigName(connString string) string {
	u, err := url.Parse(connString)
	if err != nil {
		return connString
	}
	q := u.Query()
	if q.Get("config_name") == "" {
		return connString
	}
	q.Del("config_name")
	u.RawQuery = q.Encode()
	return u.String()
}

func (s *Storage) q(table string) string {
	if s.schema != "" {
		return quoteIdent(s.schema) + "." + table
	}
	return table
}

func (s *Storage) GetSectionList() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.sections))
	for sec := range s.sections {
		out = append(out, sec)
	}
	return out
}

func (s *Storage) HasSection(section string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.sections[section]
	return ok
}

func (s *Storage) DeleteSection(section string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sections, section)
}

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

func (s *Storage) Load() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resolveLoad(context.Background())
}

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

func (s *Storage) Serialize() (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return config.SerializeConfigINI(s.sections), nil
}

func (s *Storage) Close() error {
	return s.db.Close()
}

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

var _ config.Storage = (*Storage)(nil)
