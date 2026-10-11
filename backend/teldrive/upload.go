// Package teldrive implements TelDrive v2 upload sessions.
package teldrive

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/backend/teldrive/tdhash"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/lib/pool"
	"github.com/rclone/rclone/lib/rest"
)

type storedPart struct {
	size     int64
	checksum string
}

type uploadInfo struct {
	uploadID     string
	chunkSize    int64
	expectedSize int64
	totalChunks  int64
	fileName     string
	dir          string
	storedParts  map[int]storedPart
	source       fs.ObjectInfo
	blockMu      sync.Mutex
	blockHashes  map[int][]byte
	chunkSizes   map[int]int64
	hashInvalid  bool
}

type objectChunkWriter struct {
	o          *Object
	uploadInfo *uploadInfo
}

func (w *objectChunkWriter) WriteChunk(ctx context.Context, chunkNumber int, reader io.ReadSeeker) (int64, error) {
	if chunkNumber < 0 {
		return 0, fmt.Errorf("invalid chunk number %d", chunkNumber)
	}
	var size int64
	var blocks []byte
	if w.uploadInfo.expectedSize >= 0 {
		offset := int64(chunkNumber) * w.uploadInfo.chunkSize
		if offset < 0 || offset >= w.uploadInfo.expectedSize {
			return 0, fmt.Errorf("upload chunk %d exceeds expected size", chunkNumber)
		}
		size = min(w.uploadInfo.chunkSize, w.uploadInfo.expectedSize-offset)
	} else {
		var err error
		size, err = reader.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}
		if size == 0 {
			return 0, nil
		}
		if _, err := reader.Seek(0, io.SeekStart); err != nil {
			return 0, err
		}
	}
	if stored, ok := w.uploadInfo.storedParts[chunkNumber+1]; ok {
		matches := stored.size == size
		if matches {
			if w.o.fs.opt.HashEnabled || w.uploadInfo.expectedSize < 0 {
				if w.uploadInfo.expectedSize >= 0 {
					if delay, ok := reader.(interface{ DelayAccounting(int) }); ok {
						delay.DelayAccounting(2)
					}
				}
				hasher := tdhash.New().(interface {
					io.Writer
					BlockHashes() []byte
				})
				if _, err := reader.Seek(0, io.SeekStart); err != nil {
					return 0, err
				}
				if _, err := io.CopyN(hasher, reader, size); err != nil {
					return 0, err
				}
				blocks = hasher.BlockHashes()
				if w.uploadInfo.expectedSize < 0 {
					matches = strings.EqualFold(stored.checksum, hex.EncodeToString(tdhash.SumBlockHashes(blocks)))
				}
				if _, err := reader.Seek(0, io.SeekStart); err != nil {
					return 0, err
				}
				if w.o.fs.opt.HashEnabled {
					w.uploadInfo.addBlockHashes(chunkNumber, int64(chunkNumber)*w.uploadInfo.chunkSize, size, blocks)
				}
			}
		}
		if matches {
			if w.uploadInfo.expectedSize >= 0 {
				switch r := reader.(type) {
				case *operations.ReOpen:
					r.Account(int(stored.size))
				case *pool.RW:
					r.Account(int(stored.size))
				}
			}
			fs.Debugf(w.o, "Reusing previously uploaded part %d", chunkNumber+1)
			return size, nil
		}
		resumeErr := fmt.Errorf("stored upload part %d does not match source", chunkNumber+1)
		if abortErr := w.Abort(ctx); abortErr != nil {
			return 0, errors.Join(resumeErr, fmt.Errorf("abort incompatible upload: %w", abortErr))
		}
		return 0, resumeErr
	}
	var err error
	blocks, err = w.o.putUploadPart(ctx, w.uploadInfo, chunkNumber+1, reader, size)
	if err != nil {
		return 0, err
	}
	w.uploadInfo.addBlockHashes(chunkNumber, int64(chunkNumber)*w.uploadInfo.chunkSize, size, blocks)
	return size, nil
}

func (w *objectChunkWriter) Close(ctx context.Context) error {
	file, err := w.o.completeUpload(ctx, w.uploadInfo)
	if err == nil {
		err = w.uploadInfo.setSourceHash(ctx, file)
	}
	return err
}

func (w *objectChunkWriter) Abort(ctx context.Context) error {
	return w.o.abortUpload(ctx, w.uploadInfo)
}

func (o *Object) prepareUpload(ctx context.Context, remote string, src fs.ObjectInfo) (*uploadInfo, error) {
	leaf, directoryID, err := o.fs.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}
	request := api.UploadCreateRequest{
		ParentId:          requestParentID(directoryID),
		Name:              o.fs.opt.Enc.FromStandardName(leaf),
		Size:              src.Size(),
		MimeType:          fs.MimeType(ctx, src),
		ModTime:           src.ModTime(ctx).UTC(),
		Encryption:        o.fs.opt.EncryptFiles,
		ConflictPolicy:    "replace",
		PreferredPartSize: int64(o.fs.opt.ChunkSize),
	}
	resumed, err := o.findResumableUpload(ctx, request, leaf, directoryID)
	if err != nil {
		return nil, err
	}
	if resumed != nil {
		resumed.source = src
		fs.Debugf(o.fs, "Resuming TelDrive upload %s with %d stored parts", resumed.uploadID, len(resumed.storedParts))
		return resumed, nil
	}

	opts := rest.Opts{
		Method:       http.MethodPost,
		Path:         "/api/v1/uploads",
		ExtraHeaders: map[string]string{"Idempotency-Key": uuid.NewString()},
	}
	var session api.UploadSession
	var resp *http.Response
	err = o.fs.pacer.Call(func() (bool, error) {
		var callErr error
		resp, callErr = o.fs.srv.CallJSON(ctx, &opts, &request, &session)
		return shouldRetry(ctx, resp, callErr)
	})
	if err != nil {
		return nil, err
	}
	info, err := uploadInfoFromSession(session, leaf, directoryID, nil)
	if err == nil {
		info.source = src
	}
	return info, err
}

func (o *Object) findResumableUpload(ctx context.Context, request api.UploadCreateRequest, leaf, directoryID string) (*uploadInfo, error) {
	cursor := ""
	for {
		parameters := url.Values{"state": []string{"open"}, "limit": []string{"200"}}
		if cursor != "" {
			parameters.Set("cursor", cursor)
		}
		opts := rest.Opts{Method: http.MethodGet, Path: "/api/v1/uploads", Parameters: parameters}
		var page api.UploadSessionPage
		var resp *http.Response
		err := o.fs.pacer.Call(func() (bool, error) {
			var callErr error
			resp, callErr = o.fs.srv.CallJSON(ctx, &opts, nil, &page)
			return shouldRetry(ctx, resp, callErr)
		})
		if err != nil {
			return nil, err
		}
		for _, session := range page.Items {
			if !uploadSessionMatches(session, request, time.Now().UTC()) {
				continue
			}
			parts, err := o.listUploadParts(ctx, session.ID)
			if err != nil {
				var apiErr *apiError
				if errors.As(err, &apiErr) && (apiErr.StatusCode == http.StatusNotFound || apiErr.StatusCode == http.StatusGone) {
					continue
				}
				return nil, err
			}
			stored, ok := compatibleStoredParts(session, parts)
			if !ok {
				continue
			}
			return uploadInfoFromSession(session, leaf, directoryID, stored)
		}
		if page.NextCursor == "" {
			return nil, nil
		}
		cursor = page.NextCursor
	}
}

func (o *Object) listUploadParts(ctx context.Context, uploadID string) ([]api.UploadPart, error) {
	cursor := ""
	var parts []api.UploadPart
	for {
		parameters := url.Values{"limit": []string{"200"}}
		if cursor != "" {
			parameters.Set("cursor", cursor)
		}
		opts := rest.Opts{
			Method:     http.MethodGet,
			Path:       "/api/v1/uploads/" + uploadID + "/parts",
			Parameters: parameters,
		}
		var page api.UploadPartPage
		var resp *http.Response
		err := o.fs.pacer.Call(func() (bool, error) {
			var callErr error
			resp, callErr = o.fs.srv.CallJSON(ctx, &opts, nil, &page)
			return shouldRetry(ctx, resp, callErr)
		})
		if err != nil {
			return nil, err
		}
		parts = append(parts, page.Items...)
		if page.NextCursor == "" {
			return parts, nil
		}
		cursor = page.NextCursor
	}
}

func uploadSessionMatches(session api.UploadSession, request api.UploadCreateRequest, now time.Time) bool {
	return session.State == "open" &&
		session.ID != "" && session.PartSize > 0 && session.ExpiresAt.After(now) &&
		session.ExpectedHash == nil &&
		session.ParentId == request.ParentId && session.Name == request.Name &&
		session.ExpectedSize == request.Size && session.MimeType == request.MimeType &&
		session.Encryption == request.Encryption && session.ConflictPolicy == request.ConflictPolicy &&
		session.ModTime.UTC().Truncate(time.Microsecond).Equal(request.ModTime.UTC().Truncate(time.Microsecond))
}

func compatibleStoredParts(session api.UploadSession, parts []api.UploadPart) (map[int]storedPart, bool) {
	stored := make(map[int]storedPart)
	maxPart := 0
	for _, part := range parts {
		if part.State != "stored" {
			continue
		}
		if part.PartNo < 1 || part.PlainSize <= 0 {
			return nil, false
		}
		if session.ExpectedSize >= 0 {
			expected, ok := expectedUploadPartSize(session.ExpectedSize, session.PartSize, part.PartNo)
			if !ok || part.PlainSize != expected {
				return nil, false
			}
		} else {
			checksum, err := hex.DecodeString(part.Checksum)
			if part.PlainSize > session.PartSize || err != nil || len(checksum) != tdhash.Size/2 {
				return nil, false
			}
		}
		stored[part.PartNo] = storedPart{size: part.PlainSize, checksum: part.Checksum}
		maxPart = max(maxPart, part.PartNo)
	}
	if session.ExpectedSize < 0 {
		if maxPart != len(stored) {
			return nil, false
		}
		for partNo := 1; partNo < maxPart; partNo++ {
			if stored[partNo].size != session.PartSize {
				return nil, false
			}
		}
	}
	return stored, true
}

func expectedUploadPartSize(totalSize, partSize int64, partNo int) (int64, bool) {
	if totalSize < 0 || partSize <= 0 || partNo < 1 {
		return 0, false
	}
	offset := int64(partNo-1) * partSize
	if offset >= totalSize {
		return 0, false
	}
	remaining := totalSize - offset
	if remaining < partSize {
		return remaining, true
	}
	return partSize, true
}

func uploadInfoFromSession(session api.UploadSession, leaf, directoryID string, stored map[int]storedPart) (*uploadInfo, error) {
	if session.ID == "" || session.PartSize <= 0 || session.ExpectedSize < -1 {
		return nil, fmt.Errorf("invalid TelDrive upload session")
	}
	totalChunks := int64(0)
	if session.ExpectedSize > 0 {
		totalChunks = (session.ExpectedSize + session.PartSize - 1) / session.PartSize
	}
	if stored == nil {
		stored = make(map[int]storedPart)
	}
	return &uploadInfo{
		uploadID: session.ID, chunkSize: session.PartSize, expectedSize: session.ExpectedSize, totalChunks: totalChunks,
		fileName: leaf, dir: directoryID, storedParts: stored,
	}, nil
}
func (o *Object) uploadMultipart(ctx context.Context, remote string, in io.Reader, src fs.ObjectInfo) (*uploadInfo, error) {
	info, err := o.prepareUpload(ctx, remote, src)
	if err != nil {
		return nil, err
	}
	var consumed int64
	for partNo := int64(1); partNo <= info.totalChunks; partNo++ {
		partSize := info.chunkSize
		if remaining := src.Size() - consumed; remaining < partSize {
			partSize = remaining
		}
		if _, ok := info.storedParts[int(partNo)]; ok {
			var reader io.Reader = in
			var hasher interface {
				io.Writer
				BlockHashes() []byte
			}
			if o.fs.opt.HashEnabled {
				hasher = tdhash.New().(interface {
					io.Writer
					BlockHashes() []byte
				})
				reader = io.TeeReader(in, hasher)
			}
			if _, err := io.CopyN(io.Discard, reader, partSize); err != nil {
				return nil, fmt.Errorf("skip stored upload part %d: %w", partNo, err)
			}
			if hasher != nil {
				info.addBlockHashes(int(partNo-1), consumed, partSize, hasher.BlockHashes())
			}
			consumed += partSize
			continue
		}
		blocks, err := o.putUploadPart(ctx, info, int(partNo), io.LimitReader(in, partSize), partSize)
		if err != nil {
			return nil, err
		}
		info.addBlockHashes(int(partNo-1), consumed, partSize, blocks)
		consumed += partSize
	}
	return info, nil
}

func (o *Object) putUploadPart(ctx context.Context, info *uploadInfo, partNo int, reader io.Reader, size int64) ([]byte, error) {
	if info == nil || info.uploadID == "" || partNo < 1 || size < 0 {
		return nil, fmt.Errorf("invalid TelDrive upload part")
	}
	path := fmt.Sprintf("/api/v1/uploads/%s/parts/%d", info.uploadID, partNo)
	opts := rest.Opts{
		Method: "PUT", Path: path, Body: reader, ContentLength: &size,
		ContentType: "application/octet-stream",
	}
	if o.fs.opt.UploadHost != "" {
		opts.RootURL = strings.TrimRight(o.fs.opt.UploadHost, "/") + path
		opts.Path = ""
	}
	var resp *http.Response
	var previousErr error
	attempt := 0
	var hasher interface {
		io.Writer
		Reset()
		BlockHashes() []byte
	}
	if o.fs.opt.HashEnabled {
		hasher = tdhash.New().(interface {
			io.Writer
			Reset()
			BlockHashes() []byte
		})
	}
	err := o.fs.pacer.Call(func() (bool, error) {
		if attempt > 0 {
			seeker, ok := reader.(io.Seeker)
			if !ok {
				return false, previousErr
			}
			if _, seekErr := seeker.Seek(0, io.SeekStart); seekErr != nil {
				return false, seekErr
			}
		}
		var body io.Reader = io.LimitReader(reader, size)
		if hasher != nil {
			hasher.Reset()
			body = io.TeeReader(body, hasher)
		}
		tracked := &uploadPartBody{reader: body}
		opts.Body = tracked
		attempt++
		var callErr error
		resp, callErr = o.fs.srv.Call(ctx, &opts)
		// An existing part can be acknowledged without consuming the request body.
		// lib/rest waits for the transport to close tracked before returning.
		if callErr == nil {
			var remaining int64
			remaining, callErr = io.Copy(io.Discard, body)
			if callErr == nil && tracked.n+remaining != size {
				callErr = fmt.Errorf("upload part %d: read %d bytes, expected %d: %w", partNo, tracked.n+remaining, size, io.ErrUnexpectedEOF)
			}
		}
		previousErr = callErr
		return shouldRetry(ctx, resp, callErr)
	})
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil || hasher == nil {
		return nil, err
	}
	return hasher.BlockHashes(), nil
}

type uploadPartBody struct {
	reader io.Reader
	n      int64
}

func (body *uploadPartBody) Read(p []byte) (int, error) {
	n, err := body.reader.Read(p)
	body.n += int64(n)
	return n, err
}

func (body *uploadPartBody) Close() error {
	return nil
}

func (o *Object) completeUpload(ctx context.Context, info *uploadInfo) (*api.FileInfo, error) {
	if info == nil || info.uploadID == "" {
		return nil, fmt.Errorf("invalid TelDrive upload session")
	}
	opts := rest.Opts{
		Method:       "POST",
		Path:         "/api/v1/uploads/" + info.uploadID + "/complete",
		ExtraHeaders: map[string]string{"Idempotency-Key": uuid.NewString()},
	}
	var file api.FileInfo
	var resp *http.Response
	err := o.fs.pacer.Call(func() (bool, error) {
		var callErr error
		resp, callErr = o.fs.srv.CallJSON(ctx, &opts, nil, &file)
		return shouldRetry(ctx, resp, callErr)
	})
	if err != nil {
		return nil, err
	}
	o.applyFileInfo(&file)
	return &file, nil
}

func (info *uploadInfo) addBlockHashes(chunk int, offset, size int64, blocks []byte) {
	if len(blocks) == 0 {
		return
	}
	if offset%tdhash.BlockSize != 0 || (info.expectedSize >= 0 && offset+size < info.expectedSize && size%tdhash.BlockSize != 0) || len(blocks)%32 != 0 {
		info.blockMu.Lock()
		info.hashInvalid = true
		info.blockMu.Unlock()
		return
	}
	info.blockMu.Lock()
	if info.blockHashes == nil {
		info.blockHashes = make(map[int][]byte)
	}
	if info.chunkSizes == nil {
		info.chunkSizes = make(map[int]int64)
	}
	info.blockHashes[chunk] = append([]byte(nil), blocks...)
	info.chunkSizes[chunk] = size
	info.blockMu.Unlock()
}

func (info *uploadInfo) setSourceHash(ctx context.Context, file *api.FileInfo) error {
	if info == nil || info.source == nil || file == nil {
		return nil
	}
	info.blockMu.Lock()
	if info.hashInvalid || len(info.blockHashes) == 0 {
		info.blockMu.Unlock()
		return nil
	}
	if info.expectedSize >= 0 && int64(len(info.blockHashes)) != info.totalChunks {
		info.blockMu.Unlock()
		return errors.New("incomplete upload hashes")
	}
	var blocks []byte
	var size int64
	for chunk := 0; chunk < len(info.blockHashes); chunk++ {
		hashes, ok := info.blockHashes[chunk]
		partSize := info.chunkSizes[chunk]
		if !ok || partSize <= 0 || int64(len(hashes)) != ((partSize-1)/tdhash.BlockSize+1)*32 {
			info.blockMu.Unlock()
			return errors.New("incomplete upload hashes")
		}
		size += partSize
		blocks = append(blocks, hashes...)
	}
	info.blockMu.Unlock()
	if size != file.Size || (info.expectedSize >= 0 && size != info.expectedSize) {
		return errors.New("upload hash size does not match file size")
	}
	value := hex.EncodeToString(tdhash.SumBlockHashes(blocks))
	serverValue := fileHashValue(file.Hash)
	if serverValue != "" && !strings.EqualFold(value, serverValue) {
		return fmt.Errorf("uploaded TelDrive BLAKE3 hash mismatch: source %q, server %q", value, serverValue)
	}
	if setter, ok := info.source.(fs.HashSetter); ok {
		setter.SetHash(ctx, telDriveHash, value)
	}
	return nil
}

func (o *Object) abortUpload(ctx context.Context, info *uploadInfo) error {
	if info == nil || info.uploadID == "" {
		return nil
	}
	opts := rest.Opts{Method: "DELETE", Path: "/api/v1/uploads/" + info.uploadID, NoResponse: true}
	return o.fs.pacer.Call(func() (bool, error) {
		resp, err := o.fs.srv.Call(ctx, &opts)
		return shouldRetry(ctx, resp, err)
	})
}

func (o *Object) createFile(ctx context.Context, _ fs.ObjectInfo, info *uploadInfo) error {
	_, err := o.completeUpload(ctx, info)
	return err
}
