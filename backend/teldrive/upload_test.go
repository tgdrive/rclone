package teldrive

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/backend/teldrive/tdhash"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type noHashSource struct{ fs.ObjectInfo }

func (noHashSource) Hash(context.Context, hash.Type) (string, error) {
	return "", errors.New("unexpected source hash request")
}

func newUploadTestObject(t *testing.T, handler http.HandlerFunc) *Object {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	ctx := context.Background()
	f := &Fs{
		opt:   Options{HashEnabled: true, ChunkSize: defaultChunkSize},
		pacer: fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(time.Millisecond), pacer.MaxSleep(time.Millisecond))),
	}
	f.srv = rest.NewClient(server.Client()).SetRoot(server.URL).SetErrorHandler(errorHandler)
	f.dirCache = dircache.New("", rootID, f)
	return &Object{fs: f, remote: "file.txt"}
}

func TestPrepareUploadDoesNotReadSourceHash(t *testing.T) {
	created := false
	o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/uploads":
			_, _ = io.WriteString(w, `{"items":[]}`)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/uploads":
			var request api.UploadCreateRequest
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			assert.Nil(t, request.Hash)
			created = true
			_, _ = io.WriteString(w, `{"id":"upload","partSize":4,"expectedSize":4}`)
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	src := noHashSource{object.NewStaticObjectInfo("file.txt", time.Now(), 4, true, nil, nil)}
	info, err := o.prepareUpload(context.Background(), o.remote, src)
	require.NoError(t, err)
	assert.True(t, created)
	assert.Equal(t, "upload", info.uploadID)
}

func TestPutUploadPartHashesWhileSendingWithoutChecksumHeader(t *testing.T) {
	const data = "test payload"
	var received string
	o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Empty(t, r.Header.Get("X-Part-Checksum"))
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		received = string(body)
		w.WriteHeader(http.StatusNoContent)
	})
	blocks, err := o.putUploadPart(context.Background(), &uploadInfo{uploadID: "upload"}, 1, strings.NewReader(data), int64(len(data)))
	require.NoError(t, err)
	assert.Equal(t, data, received)
	hasher := tdhash.New().(interface {
		io.Writer
		BlockHashes() []byte
	})
	_, _ = io.WriteString(hasher, data)
	assert.Equal(t, hasher.BlockHashes(), blocks)
}

func TestPutUploadPartChannelID(t *testing.T) {
	var queries []string
	var host string
	o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/uploads/upload/parts/1", r.URL.Path)
		_, _ = io.Copy(io.Discard, r.Body)
		queries = append(queries, r.URL.RawQuery)
		host = r.Host
		w.WriteHeader(http.StatusCreated)
	})
	put := func() {
		t.Helper()
		_, err := o.putUploadPart(context.Background(), &uploadInfo{uploadID: "upload"}, 1, strings.NewReader("x"), 1)
		require.NoError(t, err)
	}
	put()
	o.fs.opt.ChannelID = 9002
	put()
	o.fs.opt.UploadHost = "http://" + host + "/"
	put()
	assert.Equal(t, []string{"", "channelId=9002", "channelId=9002"}, queries)
}

func TestUploadInfoCachesVerifiedSourceHash(t *testing.T) {
	hasher := tdhash.New()
	_, _ = io.WriteString(hasher, "small file")
	blocks := hasher.(interface{ BlockHashes() []byte }).BlockHashes()
	value := hex.EncodeToString(tdhash.SumBlockHashes(blocks))
	source := &hashSetterSource{}
	info := &uploadInfo{
		source: source, expectedSize: 10, chunkSize: 10, totalChunks: 1,
		blockHashes: map[int][]byte{0: blocks},
		chunkSizes:  map[int]int64{0: 10},
	}
	err := info.setSourceHash(context.Background(), &api.FileInfo{Size: 10, Hash: &api.FileHash{Algorithm: "blake3", Value: value}})
	require.NoError(t, err)
	assert.Equal(t, telDriveHash, source.ty)
	assert.Equal(t, value, source.value)
	info.source = &hashSetterSource{}
	err = info.setSourceHash(context.Background(), &api.FileInfo{Size: 10, Hash: &api.FileHash{Algorithm: "blake3", Value: "wrong"}})
	require.ErrorContains(t, err, "hash mismatch")
	assert.Empty(t, info.source.(*hashSetterSource).value)
}

func TestUnknownSizeResumeChecksum(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, matches := range []bool{false, true} {
			t.Run(fmt.Sprintf("hash=%v/matches=%v", enabled, matches), func(t *testing.T) {
				aborted := false
				o := newUploadTestObject(t, func(w http.ResponseWriter, r *http.Request) {
					assert.Equal(t, http.MethodDelete, r.Method)
					aborted = true
					w.WriteHeader(http.StatusNoContent)
				})
				o.fs.opt.HashEnabled = enabled
				data := "old!"
				if !matches {
					data = "new!"
				}
				writer := &objectChunkWriter{o: o, uploadInfo: &uploadInfo{
					uploadID: "upload", chunkSize: 4, expectedSize: -1,
					storedParts: map[int]storedPart{1: {size: 4, checksum: tdhash.SumString([]byte("old!"))}},
				}}
				n, err := writer.WriteChunk(context.Background(), 0, strings.NewReader(data))
				if matches {
					require.NoError(t, err)
					assert.Equal(t, int64(4), n)
				} else {
					require.ErrorContains(t, err, "does not match source")
				}
				assert.Equal(t, !matches, aborted)
			})
		}
	}
}

type uploadTestTransport func(*http.Request) (*http.Response, error)

func (f uploadTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestUploadPartEarlyResponse(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			const data = "complete payload"
			o := newUploadTestObject(t, func(http.ResponseWriter, *http.Request) { t.Error("unexpected HTTP request") })
			o.fs.opt.HashEnabled = enabled
			o.fs.srv = rest.NewClient(&http.Client{Transport: uploadTestTransport(func(r *http.Request) (*http.Response, error) {
				defer r.Body.Close()
				_, err := io.CopyN(io.Discard, r.Body, 1)
				if err != nil {
					return nil, err
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
			})}).SetRoot("http://fake")
			reader := strings.NewReader(data + "next part")
			blocks, err := o.putUploadPart(context.Background(), &uploadInfo{uploadID: "upload"}, 1, reader, int64(len(data)))
			require.NoError(t, err)
			if enabled {
				assert.Equal(t, tdhash.SumString([]byte(data)), hex.EncodeToString(tdhash.SumBlockHashes(blocks)))
			}
			rest, err := io.ReadAll(reader)
			require.NoError(t, err)
			assert.Equal(t, "next part", string(rest))
		})
	}
}

func TestUploadHashCoverage(t *testing.T) {
	data := bytes.Repeat([]byte("x"), tdhash.BlockSize+4)
	for _, missing := range []int{-1, 0, 1} {
		t.Run(fmt.Sprint(missing), func(t *testing.T) {
			source := &hashSetterSource{}
			info := &uploadInfo{source: source, expectedSize: int64(len(data)), chunkSize: tdhash.BlockSize, totalChunks: 2}
			for _, chunk := range []int{1, 0} {
				if chunk == missing {
					continue
				}
				start := chunk * tdhash.BlockSize
				part := data[start:min(start+tdhash.BlockSize, len(data))]
				h := tdhash.New()
				_, _ = h.Write(part)
				info.addBlockHashes(chunk, int64(start), int64(len(part)), h.(interface{ BlockHashes() []byte }).BlockHashes())
			}
			file := &api.FileInfo{Size: int64(len(data)), Hash: &api.FileHash{Algorithm: "blake3", Value: tdhash.SumString(data)}}
			err := info.setSourceHash(context.Background(), file)
			if missing < 0 {
				require.NoError(t, err)
				assert.Equal(t, file.Hash.Value, source.value)
			} else {
				require.Error(t, err)
				assert.Empty(t, source.value)
			}
		})
	}
}

func TestUploadPartRetryAndShortSource(t *testing.T) {
	for _, short := range []bool{false, true} {
		t.Run(fmt.Sprint(short), func(t *testing.T) {
			attempts := 0
			o := newUploadTestObject(t, func(http.ResponseWriter, *http.Request) { t.Error("unexpected HTTP request") })
			o.fs.srv = rest.NewClient(&http.Client{Transport: uploadTestTransport(func(r *http.Request) (*http.Response, error) {
				defer r.Body.Close()
				attempts++
				status := http.StatusOK
				if !short && attempts == 1 {
					_, err := io.Copy(io.Discard, r.Body)
					require.NoError(t, err)
					status = http.StatusServiceUnavailable
				}
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
			})}).SetRoot("http://fake").SetErrorHandler(errorHandler)
			data := "retry payload"
			size := int64(len(data))
			if short {
				size++
			}
			blocks, err := o.putUploadPart(context.Background(), &uploadInfo{uploadID: "upload"}, 1, strings.NewReader(data), size)
			if short {
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
				assert.Empty(t, blocks)
			} else {
				require.NoError(t, err)
				assert.Equal(t, 2, attempts)
				assert.Equal(t, tdhash.SumString([]byte(data)), hex.EncodeToString(tdhash.SumBlockHashes(blocks)))
			}
		})
	}
}

type hashSetterSource struct {
	ty    hash.Type
	value string
}

func (*hashSetterSource) Fs() fs.Info                       { return nil }
func (*hashSetterSource) String() string                    { return "source" }
func (*hashSetterSource) Remote() string                    { return "source" }
func (*hashSetterSource) ModTime(context.Context) time.Time { return time.Time{} }
func (*hashSetterSource) Size() int64                       { return 0 }
func (*hashSetterSource) Hash(context.Context, hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}
func (*hashSetterSource) Storable() bool { return true }
func (s *hashSetterSource) SetHash(_ context.Context, ty hash.Type, value string) {
	s.ty, s.value = ty, value
}
