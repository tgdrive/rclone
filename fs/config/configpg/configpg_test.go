package configpg

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Ensure unique schema per package run — configpg_test is used.
const testSchema = "configpg_test"

// testDBURL is the PostgreSQL connection string for tests.
// Override via RCLONE_TEST_DB env var. Uses a dedicated search_path so tests
// don't interfere with real data.
var testDBURL = "postgres://test:test@localhost:5432/postgres?search_path=" + testSchema

// TestMain initializes the test schema once before all tests.
func TestMain(m *testing.M) {
	ctx := context.Background()
	dbURL := testDBURL
	if e := os.Getenv("RCLONE_TEST_DB"); e != "" {
		dbURL = e
	}
	rawDB, err := openPG(rewriteSearchPath(dbURL, "public"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "configpg test: cannot connect to PG: %v. Set RCLONE_TEST_DB or start a local PG.\n", err)
		os.Exit(1)
	}
	_, _ = rawDB.ExecContext(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", quoteIdent(testSchema)))
	_, _ = rawDB.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA %s", quoteIdent(testSchema)))
	rawDB.Close()
	os.Exit(m.Run())
}

// newStorage creates a Storage connected to the test schema. Tables are
// truncated after the test via t.Cleanup so each test gets a clean slate
// while still allowing multi-Storage round trips within a single test.
func newStorage(t *testing.T, ctx context.Context) *Storage {
	t.Helper()
	dbURL := testDBURL
	if e := os.Getenv("RCLONE_TEST_DB"); e != "" {
		dbURL = e
	}
	s, err := NewStorage(ctx, dbURL)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = s.db.ExecContext(ctx, "TRUNCATE TABLE rclone_config CASCADE")
		_, _ = s.db.ExecContext(ctx, "TRUNCATE TABLE rclone_config_encrypted CASCADE")
		s.Close()
	})
	return s
}

// rewriteSearchPath replaces the search_path parameter in a PG URL.
func rewriteSearchPath(connString, newPath string) string {
	if strings.Contains(connString, "?search_path=") {
		before, _, _ := strings.Cut(connString, "?search_path=")
		// remove any trailing parameters after & and keep them
		return before + "?search_path=" + newPath
	}
	if strings.Contains(connString, "?") {
		return connString + "&search_path=" + newPath
	}
	return connString + "?search_path=" + newPath
}

// withPassword sets the configKey directly and returns a cleanup that clears it.
func withPassword(t *testing.T, password string) func() {
	t.Helper()
	err := config.SetConfigPassword(password)
	require.NoError(t, err)
	return func() { config.ClearConfigPassword() }
}

// withoutPassword clears the configKey. The return value is a no-op — it's
// kept so call sites that defer it stay readable.
func withoutPassword(t *testing.T) func() {
	t.Helper()
	config.ClearConfigPassword()
	return func() {}
}



// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestEmptyStorage(t *testing.T) {
	ctx := context.Background()
	s := newStorage(t, ctx)

	// Load on empty DB should return empty, not error
	err := s.Load()
	require.NoError(t, err)
	assert.Equal(t, []string{}, s.GetSectionList())
	assert.False(t, s.HasSection("nonexistent"))
}

func TestUnencryptedRoundTrip(t *testing.T) {
	ctx := context.Background()
	defer withoutPassword(t)()

	// Start fresh
	s := newStorage(t, ctx)
	err := s.Load()
	require.NoError(t, err)

	// Set values
	s.SetValue("remote1", "type", "drive")
	s.SetValue("remote1", "client_id", "abc123")
	s.SetValue("remote2", "type", "s3")
	s.SetValue("remote2", "provider", "AWS")

	// Read back from cache
	v, found := s.GetValue("remote1", "type")
	assert.True(t, found)
	assert.Equal(t, "drive", v)
	assert.True(t, s.HasSection("remote1"))
	assert.ElementsMatch(t, []string{"client_id", "type"}, s.GetKeyList("remote1"))

	// Save to persist rows
	err = s.Save()
	require.NoError(t, err)

	// Load a fresh storage from DB
	s2 := newStorage(t, ctx)
	err = s2.Load()
	require.NoError(t, err)

	v, found = s2.GetValue("remote1", "type")
	assert.True(t, found)
	assert.Equal(t, "drive", v)

	v, found = s2.GetValue("remote2", "provider")
	assert.True(t, found)
	assert.Equal(t, "AWS", v)

	// Verify no encrypted blob exists
	assert.False(t, s2.hasEncryptedBlob(ctx))
}

func TestEncryptionRoundTrip(t *testing.T) {
	ctx := context.Background()
	cleanup := withPassword(t, "test-password-123")
	defer cleanup()

	s := newStorage(t, ctx)
	err := s.Load()
	require.NoError(t, err)

	// Set and save values
	s.SetValue("secretremote", "type", "drive")
	s.SetValue("secretremote", "token", "sensitive-data")
	err = s.Save()
	require.NoError(t, err)

	// Verify individual rows table is empty (encrypted blob is canonical)
	var rowCount int
	err = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rclone_config").Scan(&rowCount)
	require.NoError(t, err)
	assert.Equal(t, 0, rowCount, "individual rows should be empty when encrypted")

	// Verify encrypted blob exists
	assert.True(t, s.hasEncryptedBlob(ctx))

	// Load into a fresh storage with the correct password
	s2 := newStorage(t, ctx)
	err = s2.Load()
	require.NoError(t, err)

	v, found := s2.GetValue("secretremote", "type")
	assert.True(t, found)
	assert.Equal(t, "drive", v)
	v, found = s2.GetValue("secretremote", "token")
	assert.True(t, found)
	assert.Equal(t, "sensitive-data", v)
}

func TestEncryptionWrongPassword(t *testing.T) {
	ctx := context.Background()

	// Save with correct password
	cleanup := withPassword(t, "correct-password")
	defer cleanup()

	s := newStorage(t, ctx)
	err := s.Load()
	require.NoError(t, err)
	s.SetValue("secretremote", "type", "drive")
	err = s.Save()
	require.NoError(t, err)
	config.ClearConfigPassword()

	// Disable interactive prompting so Decrypt returns an error instead
	ci := fs.GetConfig(ctx)
	oldAsk := ci.AskPassword
	ci.AskPassword = false
	defer func() { ci.AskPassword = oldAsk }()

	err = config.SetConfigPassword("wrong-password")
	require.NoError(t, err)
	defer config.ClearConfigPassword()

	s2 := newStorage(t, ctx)
	err = s2.Load()
	require.Error(t, err)
}

func TestEncryptionRemove(t *testing.T) {
	ctx := context.Background()
	cleanup := withPassword(t, "test-password-456")
	defer cleanup()

	s := newStorage(t, ctx)
	err := s.Load()
	require.NoError(t, err)
	s.SetValue("remote", "type", "drive")
	s.SetValue("remote", "token", "secret123")
	err = s.Save()
	require.NoError(t, err)

	// Verify encrypted blob
	assert.True(t, s.hasEncryptedBlob(ctx))

	// Remove password and save (this should flush to individual rows + remove blob)
	cleanup() // clears configKey

	err = s.Save()
	require.NoError(t, err)

	// Verify blob is gone
	assert.False(t, s.hasEncryptedBlob(ctx))

	// Verify individual rows are populated
	var count int
	err = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rclone_config").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 2, count)

	// Load into fresh storage without any password
	s2 := newStorage(t, ctx)
	err = s2.Load()
	require.NoError(t, err)

	v, found := s2.GetValue("remote", "type")
	assert.True(t, found)
	assert.Equal(t, "drive", v)
	v, found = s2.GetValue("remote", "token")
	assert.True(t, found)
	assert.Equal(t, "secret123", v)
}

func TestSectionCRUD(t *testing.T) {
	ctx := context.Background()
	defer withoutPassword(t)()

	s := newStorage(t, ctx)
	err := s.Load()
	require.NoError(t, err)

	// Start empty
	assert.False(t, s.HasSection("remote"))
	assert.Equal(t, []string{}, s.GetSectionList())

	// Add some data
	s.SetValue("remote", "type", "drive")
	s.SetValue("remote", "token", "abc")
	assert.True(t, s.HasSection("remote"))
	assert.Equal(t, []string{"remote"}, s.GetSectionList())
	assert.ElementsMatch(t, []string{"token", "type"}, s.GetKeyList("remote"))

	// Delete single key
	deleted := s.DeleteKey("remote", "token")
	assert.True(t, deleted)
	assert.Equal(t, []string{"type"}, s.GetKeyList("remote"))

	// Delete non-existent key returns false
	deleted = s.DeleteKey("remote", "nonexistent")
	assert.False(t, deleted)

	// Delete section
	s.DeleteSection("remote")
	assert.False(t, s.HasSection("remote"))
	assert.Equal(t, []string{}, s.GetSectionList())

	// Save and reload
	err = s.Save()
	require.NoError(t, err)

	s2 := newStorage(t, ctx)
	err = s2.Load()
	require.NoError(t, err)
	assert.False(t, s2.HasSection("remote"))
}

func TestVirtualBackendRejected(t *testing.T) {
	ctx := context.Background()
	defer withoutPassword(t)()

	s := newStorage(t, ctx)
	err := s.Load()
	require.NoError(t, err)

	// :backend: sections should be silently rejected
	s.SetValue(":backend:http", "type", "http")
	s.SetValue(":backend:s3", "vendor", "AWS")

	assert.False(t, s.HasSection(":backend:http"))
	assert.False(t, s.HasSection(":backend:s3"))
}

func TestDeleteNonexistentSection(t *testing.T) {
	ctx := context.Background()
	defer withoutPassword(t)()

	s := newStorage(t, ctx)
	err := s.Load()
	require.NoError(t, err)

	// Deleting a section that doesn't exist should not panic
	s.DeleteSection("doesnotexist")
	assert.False(t, s.HasSection("doesnotexist"))
}

func TestGetValueNonexistent(t *testing.T) {
	ctx := context.Background()
	defer withoutPassword(t)()

	s := newStorage(t, ctx)
	err := s.Load()
	require.NoError(t, err)

	v, found := s.GetValue("nosection", "nokey")
	assert.False(t, found)
	assert.Equal(t, "", v)

	s.SetValue("remote", "type", "drive")
	v, found = s.GetValue("remote", "nonexistent")
	assert.False(t, found)
	assert.Equal(t, "", v)
}

func TestOverwriteValue(t *testing.T) {
	ctx := context.Background()
	defer withoutPassword(t)()

	s := newStorage(t, ctx)
	err := s.Load()
	require.NoError(t, err)

	s.SetValue("remote", "type", "drive")
	s.SetValue("remote", "type", "s3")

	v, found := s.GetValue("remote", "type")
	assert.True(t, found)
	assert.Equal(t, "s3", v)
}

func TestSaveNoopWithoutData(t *testing.T) {
	ctx := context.Background()
	defer withoutPassword(t)()

	s := newStorage(t, ctx)
	err := s.Load()
	require.NoError(t, err)

	// Save with no data should be a no-op
	err = s.Save()
	require.NoError(t, err)

	// Reload should still be empty
	s2 := newStorage(t, ctx)
	err = s2.Load()
	require.NoError(t, err)
	assert.Equal(t, []string{}, s2.GetSectionList())
}

func TestSerialize(t *testing.T) {
	ctx := context.Background()
	defer withoutPassword(t)()

	s := newStorage(t, ctx)
	err := s.Load()
	require.NoError(t, err)

	s.SetValue("remote", "type", "drive")
	s.SetValue("remote", "token", "abc")

	serialized, err := s.Serialize()
	require.NoError(t, err)
	assert.Contains(t, serialized, `"remote"`)
	assert.Contains(t, serialized, `"drive"`)
	assert.Contains(t, serialized, `"abc"`)
}

func TestClose(t *testing.T) {
	ctx := context.Background()
	defer withoutPassword(t)()

	s := newStorage(t, ctx)
	// Close should succeed
	err := s.Close()
	require.NoError(t, err)

	// Multiple closes should not panic or error
	err = s.Close()
	require.NoError(t, err)
}

func TestSchemaAutoCreation(t *testing.T) {
	ctx := context.Background()
	defer withoutPassword(t)()

	// Use a URL with a different search_path to trigger schema creation
	schemaURL := rewriteSearchPath(testDBURL, "configpg_test_schema_auto")
	rawDB, err := openPG(rewriteSearchPath(testDBURL, "public"))
	require.NoError(t, err)
	defer rawDB.Close()
	_, _ = rawDB.ExecContext(ctx, "DROP SCHEMA IF EXISTS configpg_test_schema_auto CASCADE")

	s, err := NewStorage(ctx, schemaURL)
	require.NoError(t, err)
	defer func() { _ = s.Close() }()

	// Verify the schema was created and tables exist in it
	var schemaName string
	err = s.db.QueryRowContext(ctx, "SELECT current_schema").Scan(&schemaName)
	require.NoError(t, err)
	assert.Equal(t, "configpg_test_schema_auto", schemaName)

	// Verify we can read/write through the qualified tables
	s.Load()
	s.SetValue("test", "key", "value")
	err = s.Save()
	require.NoError(t, err)

	s2, err := NewStorage(ctx, schemaURL)
	require.NoError(t, err)
	defer func() { _ = s2.Close() }()

	err = s2.Load()
	require.NoError(t, err)
	v, found := s2.GetValue("test", "key")
	assert.True(t, found)
	assert.Equal(t, "value", v)
}


