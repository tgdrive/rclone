package teldrive

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"

	"github.com/google/uuid"
	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/lib/pool"
	"github.com/rclone/rclone/lib/rest"

	"github.com/rclone/rclone/fs"
)

// memoryBufferThreshold is the size limit for memory buffering
// Files smaller than this will be buffered in memory, larger files use temp file
const memoryBufferThreshold = 10 * 1024 * 1024 // 10MB

type uploadInfo struct {
	existingChunks map[int]api.PartFile
	uploadID       string
	channelID      int64
	encryptFile    bool
	chunkSize      int64
	totalChunks    int64
	fileName       string
	dir            string
}

type objectChunkWriter struct {
	size       int64
	f          *Fs
	src        fs.ObjectInfo
	o          *Object
	uploadInfo *uploadInfo
}

func getMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

// WriteChunk will write chunk number with reader bytes, where chunk number >= 0
func (w *objectChunkWriter) WriteChunk(ctx context.Context, chunkNumber int, reader io.ReadSeeker) (size int64, err error) {
	if chunkNumber < 0 {
		err := fmt.Errorf("invalid chunk number provided: %v", chunkNumber)
		return -1, err
	}

	chunkNumber += 1

	if existing, ok := w.uploadInfo.existingChunks[chunkNumber]; ok {
		switch r := reader.(type) {
		case *operations.ReOpen:
			r.Account(int(existing.Size))
		case *pool.RW:
			r.Account(int(existing.Size))
		default:
		}
		return existing.Size, nil
	}

	var partName string

	err = w.f.pacer.Call(func() (bool, error) {

		size, err = reader.Seek(0, io.SeekEnd)
		if err != nil {

			return false, err
		}

		_, err = reader.Seek(0, io.SeekStart)
		if err != nil {
			return false, err
		}

		fs.Debugf(w.o, "Sending chunk %d length %d", chunkNumber, size)
		if w.f.opt.RandomChunkName {
			partName = getMD5Hash(uuid.New().String())
		} else {
			partName = w.uploadInfo.fileName
			if w.uploadInfo.totalChunks > 1 {
				partName = fmt.Sprintf("%s.part.%03d", w.uploadInfo.fileName, chunkNumber)
			}
		}

		opts := rest.Opts{
			Method:        "POST",
			Body:          reader,
			ContentLength: &size,
			NoResponse:    true,
			ContentType:   "application/octet-stream",
			Parameters: url.Values{
				"partName":  []string{partName},
				"fileName":  []string{w.uploadInfo.fileName},
				"partNo":    []string{strconv.Itoa(chunkNumber)},
				"channelId": []string{strconv.FormatInt(w.uploadInfo.channelID, 10)},
				"encrypted": []string{strconv.FormatBool(w.uploadInfo.encryptFile)},
			},
		}

		if w.f.opt.HashEnabled {
			opts.Parameters.Set("hashing", "true")
		}

		if w.f.opt.UploadHost != "" {
			opts.RootURL = w.f.opt.UploadHost + "/api/uploads/" + w.uploadInfo.uploadID

		} else {
			opts.Path = "/api/uploads/" + w.uploadInfo.uploadID
		}

		resp, err := w.f.srv.Call(ctx, &opts)

		retry, err := shouldRetry(ctx, resp, err)

		if err != nil {
			fs.Debugf(w.o, "Error sending chunk %d (retry=%v): %v: %#v", chunkNumber, retry, err, err)
		}

		return retry, err

	})

	if err != nil {
		return 0, fmt.Errorf("error sending chunk %d: %v", chunkNumber, err)
	}

	fs.Debugf(w.o, "Done sending chunk %d", chunkNumber)

	return size, err

}

func (w *objectChunkWriter) Close(ctx context.Context) error {

	return w.o.createFile(ctx, w.src, w.uploadInfo)
}

func (*objectChunkWriter) Abort(ctx context.Context) error {
	return nil
}

// chunkFile stores a single chunk's temp file path and size
type chunkFile struct {
	tempPath string
	size     int64
}

// bufferingChunkWriter handles uploads with unknown size by buffering chunks to temp files
// Used by OpenChunkWriter for streaming uploads
// Supports out-of-order chunks - stores each chunk in separate temp file and reassembles in order at Close()
type bufferingChunkWriter struct {
	f         *Fs
	o         *Object
	src       fs.ObjectInfo
	remote    string
	chunks    map[int]*chunkFile // Store temp file paths by chunk number
	totalSize int64
	maxChunk  int // Track highest chunk number seen
}

// WriteChunk stores a chunk to a temp file by its chunk number (supports out-of-order writes)
func (w *bufferingChunkWriter) WriteChunk(ctx context.Context, chunkNumber int, reader io.ReadSeeker) (size int64, err error) {
	// Create temp file for this chunk
	tempFile, err := os.CreateTemp("", fmt.Sprintf("rclone-teldrive-chunk-%d-*", chunkNumber))
	if err != nil {
		return 0, fmt.Errorf("failed to create temp file for chunk %d: %w", chunkNumber, err)
	}

	tempPath := tempFile.Name()

	// Copy data to temp file
	size, err = io.Copy(tempFile, reader)
	if err != nil {
		tempFile.Close()
		_ = os.Remove(tempPath)
		return 0, fmt.Errorf("failed to write chunk %d to temp file: %w", chunkNumber, err)
	}

	// Close temp file (we'll reopen for reading in Close)
	if err := tempFile.Close(); err != nil {
		_ = os.Remove(tempPath)
		return 0, fmt.Errorf("failed to close temp file for chunk %d: %w", chunkNumber, err)
	}

	// Store chunk info (file will be deleted in Close or Abort)
	w.chunks[chunkNumber] = &chunkFile{
		tempPath: tempPath,
		size:     size,
	}
	w.totalSize += size

	// Track highest chunk number
	if chunkNumber > w.maxChunk {
		w.maxChunk = chunkNumber
	}

	fs.Debugf(w.f, "Buffered chunk %d: %d bytes to temp file %s", chunkNumber, size, tempPath)
	return size, nil
}

// Close reassembles all chunks in order and uploads to TelDrive
func (w *bufferingChunkWriter) Close(ctx context.Context) error {
	fs.Debugf(w.f, "Closing bufferingChunkWriter: %d chunks, total size %d bytes", len(w.chunks), w.totalSize)

	// Create a reader that yields chunks in order (0, 1, 2, ...)
	chunkReader := &orderedChunkReader{
		chunks:   w.chunks,
		maxChunk: w.maxChunk,
		current:  0,
	}

	// Create source info with known size
	src := object.NewStaticObjectInfo(w.remote, w.src.ModTime(ctx), w.totalSize, false, nil, w.f)

	// Upload using the ordered chunk reader
	uploadInfo, err := w.o.uploadMultipart(ctx, chunkReader, src)
	if err != nil {
		return fmt.Errorf("failed to upload buffered chunks: %w", err)
	}

	// Clean up temp files
	chunkReader.cleanup()
	w.chunks = nil

	return w.o.createFile(ctx, src, uploadInfo)
}

// Abort cleans up temp files
func (w *bufferingChunkWriter) Abort(ctx context.Context) error {
	for _, chunk := range w.chunks {
		if chunk.tempPath != "" {
			_ = os.Remove(chunk.tempPath)
		}
	}
	w.chunks = nil
	return nil
}

// orderedChunkReader reads chunks in order (0, 1, 2, ...) from temp files
type orderedChunkReader struct {
	chunks    map[int]*chunkFile
	maxChunk  int
	current   int
	file      *os.File
	remaining int64
}

func (r *orderedChunkReader) Read(p []byte) (n int, err error) {
	for r.current <= r.maxChunk {
		// Open next chunk file if needed
		if r.file == nil {
			chunk, ok := r.chunks[r.current]
			if !ok {
				// Skip missing chunks (shouldn't happen in normal operation)
				r.current++
				continue
			}

			file, err := os.Open(chunk.tempPath)
			if err != nil {
				return 0, fmt.Errorf("failed to open chunk %d temp file: %w", r.current, err)
			}
			r.file = file
			r.remaining = chunk.size
		}

		// Read from current chunk file
		n, err = r.file.Read(p)
		if n > 0 {
			r.remaining -= int64(n)
			if r.remaining <= 0 {
				// Finished this chunk, close and move to next
				r.file.Close()
				r.file = nil
				r.current++
			}
			return n, nil
		}
		if err != nil && err != io.EOF {
			return 0, err
		}

		// EOF on this chunk, close and move to next
		r.file.Close()
		r.file = nil
		r.current++
	}

	return 0, io.EOF
}

func (r *orderedChunkReader) cleanup() {
	if r.file != nil {
		r.file.Close()
	}
	for _, chunk := range r.chunks {
		if chunk.tempPath != "" {
			_ = os.Remove(chunk.tempPath)
		}
	}
}

// uploadWithBuffering buffers data to memory or temp file for unknown-sized uploads
// Returns the uploadInfo, final size, and any error
func (o *Object) uploadWithBuffering(ctx context.Context, in io.Reader, src fs.ObjectInfo) (*uploadInfo, int64, error) {
	var buffer bytes.Buffer
	var tempFile *os.File
	var written int64

	// Read data in chunks
	buf := make([]byte, 64*1024) // 64KB read buffer
	for {
		n, err := in.Read(buf)
		if n > 0 {
			// Check if we need to switch to temp file
			if tempFile == nil && written+int64(n) > memoryBufferThreshold {
				fs.Debugf(o, "Buffering: switching to temp file at %d bytes", written+int64(n))
				tempFile, err = os.CreateTemp("", "rclone-teldrive-*")
				if err != nil {
					return nil, 0, fmt.Errorf("failed to create temp file: %w", err)
				}
				_ = os.Remove(tempFile.Name()) // Delete immediately (Unix trick)

				// Copy existing buffer to temp file
				if buffer.Len() > 0 {
					_, err = tempFile.Write(buffer.Bytes())
					if err != nil {
						tempFile.Close()
						return nil, 0, fmt.Errorf("failed to copy buffer to temp file: %w", err)
					}
					buffer = bytes.Buffer{} // Free memory
				}
			}

			// Write to appropriate target
			if tempFile != nil {
				_, err = tempFile.Write(buf[:n])
				if err != nil {
					tempFile.Close()
					return nil, 0, fmt.Errorf("failed to write to temp file: %w", err)
				}
			} else {
				buffer.Write(buf[:n])
			}
			written += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			if tempFile != nil {
				tempFile.Close()
			}
			return nil, 0, fmt.Errorf("failed to read input: %w", err)
		}
	}

	// Now upload with known size
	var uploadInfo *uploadInfo
	var err error

	if tempFile != nil {
		// Upload from temp file
		defer func() {
			_ = tempFile.Close()
		}()

		_, err = tempFile.Seek(0, io.SeekStart)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to seek temp file: %w", err)
		}

		fs.Debugf(o, "Uploading %d bytes from temp file", written)
		srcWithSize := object.NewStaticObjectInfo(src.Remote(), src.ModTime(ctx), written, false, nil, o.fs)
		uploadInfo, err = o.uploadMultipart(ctx, tempFile, srcWithSize)
	} else {
		// Upload from memory buffer
		fs.Debugf(o, "Uploading %d bytes from memory buffer", written)
		srcWithSize := object.NewStaticObjectInfo(src.Remote(), src.ModTime(ctx), written, false, nil, o.fs)
		uploadInfo, err = o.uploadMultipart(ctx, bytes.NewReader(buffer.Bytes()), srcWithSize)
	}

	if err != nil {
		return nil, 0, err
	}

	return uploadInfo, written, nil
}

func (o *Object) prepareUpload(ctx context.Context, src fs.ObjectInfo) (*uploadInfo, error) {

	leaf, directoryID, err := o.fs.dirCache.FindPath(ctx, src.Remote(), true)

	if err != nil {
		return nil, err
	}

	uploadID := getMD5Hash(fmt.Sprintf("%s:%s:%d:%d", directoryID, leaf, src.Size(), o.fs.userId))

	var (
		uploadParts    []api.PartFile
		existingChunks map[int]api.PartFile
	)

	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/uploads/" + uploadID,
	}

	chunkSize := int64(o.fs.opt.ChunkSize)

	if chunkSize < src.Size() {
		err := o.fs.pacer.Call(func() (bool, error) {
			resp, err := o.fs.srv.CallJSON(ctx, &opts, nil, &uploadParts)
			return shouldRetry(ctx, resp, err)
		})

		if err != nil {
			return nil, err
		}
		existingChunks = make(map[int]api.PartFile, len(uploadParts))
		for _, part := range uploadParts {
			existingChunks[part.PartNo] = part
		}

	}

	totalChunks := src.Size() / chunkSize

	if src.Size()%chunkSize != 0 {
		totalChunks++
	}

	channelID := o.fs.opt.ChannelID

	encryptFile := o.fs.opt.EncryptFiles

	if len(uploadParts) > 0 {
		channelID = uploadParts[0].ChannelID
		encryptFile = uploadParts[0].Encrypted
	}

	return &uploadInfo{
		existingChunks: existingChunks,
		uploadID:       uploadID,
		channelID:      channelID,
		encryptFile:    encryptFile,
		chunkSize:      chunkSize,
		totalChunks:    totalChunks,
		fileName:       leaf,
		dir:            directoryID,
	}, nil
}

func (o *Object) uploadMultipart(ctx context.Context, in io.Reader, src fs.ObjectInfo) (*uploadInfo, error) {

	size := src.Size()

	if size < 0 {
		return nil, errors.New("unknown-sized upload not supported")
	}

	uploadInfo, err := o.prepareUpload(ctx, src)

	if err != nil {
		return nil, err
	}

	if size > 0 {

		var (
			partsToCommit []api.PartFile
			uploadedSize  int64
		)

		totalChunks := int(uploadInfo.totalChunks)

		for chunkNo := 1; chunkNo <= totalChunks; chunkNo++ {
			if existing, ok := uploadInfo.existingChunks[chunkNo]; ok {
				io.CopyN(io.Discard, in, existing.Size)
				partsToCommit = append(partsToCommit, existing)
				uploadedSize += existing.Size
				continue
			}

			n := uploadInfo.chunkSize

			if chunkNo == totalChunks {
				n = src.Size() - uploadedSize
			}

			chunkName := uploadInfo.fileName

			if o.fs.opt.RandomChunkName {
				chunkName = getMD5Hash(uuid.New().String())
			} else if totalChunks > 1 {
				chunkName = fmt.Sprintf("%s.part.%03d", chunkName, chunkNo)
			}

			partReader := io.LimitReader(in, n)

			opts := rest.Opts{
				Method:        "POST",
				Body:          partReader,
				NoResponse:    true,
				ContentLength: &n,
				ContentType:   "application/octet-stream",
				Parameters: url.Values{
					"partName":  []string{chunkName},
					"fileName":  []string{uploadInfo.fileName},
					"partNo":    []string{strconv.Itoa(chunkNo)},
					"channelId": []string{strconv.FormatInt(uploadInfo.channelID, 10)},
					"encrypted": []string{strconv.FormatBool(uploadInfo.encryptFile)},
				},
			}

			if o.fs.opt.HashEnabled {
				opts.Parameters.Set("hashing", "true")
			}

			if o.fs.opt.UploadHost != "" {
				opts.RootURL = o.fs.opt.UploadHost + "/api/uploads/" + uploadInfo.uploadID

			} else {
				opts.Path = "/api/uploads/" + uploadInfo.uploadID
			}
			_, err := o.fs.srv.Call(ctx, &opts)
			if err != nil {
				return nil, err
			}
			uploadedSize += n
		}

	}
	return uploadInfo, nil

}

func (o *Object) createFile(ctx context.Context, src fs.ObjectInfo, uploadInfo *uploadInfo) error {

	opts := rest.Opts{
		Method:     "POST",
		Path:       "/api/files",
		NoResponse: true,
	}

	payload := api.CreateFileRequest{
		Name:      uploadInfo.fileName,
		Type:      "file",
		ParentId:  uploadInfo.dir,
		MimeType:  fs.MimeType(ctx, src),
		Size:      src.Size(),
		UploadId:  uploadInfo.uploadID,
		ChannelID: uploadInfo.channelID,
		Encrypted: uploadInfo.encryptFile,
		ModTime:   src.ModTime(ctx).UTC(),
	}

	err := o.fs.pacer.Call(func() (bool, error) {
		resp, err := o.fs.srv.CallJSON(ctx, &opts, &payload, nil)
		return shouldRetry(ctx, resp, err)
	})

	if err != nil {
		return err
	}

	return nil
}
