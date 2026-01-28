package operations

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/atexit"
	"github.com/rclone/rclone/lib/multipart"
	"github.com/rclone/rclone/lib/pool"
	"golang.org/x/sync/errgroup"
)

const (
	multithreadChunkSize = 64 << 10
	resumeStateSuffix    = ".rclone"
	resumeFileVersion    = 1
)

// resumeState holds the resume state using a bitmap
type resumeState struct {
	Source      string
	Destination string
	Size        int64
	ChunkSize   int64
	NumChunks   int
	ETag        string    // ETag from source (if available)
	ModTime     time.Time // Modification time from source
	// Bitmap: 1 bit per chunk, 1=complete, 0=missing
	Bitmap []byte
}

// loadResumeState loads resume state from binary file
func loadResumeState(filePath string) (*resumeState, error) {
	statePath := filePath + resumeStateSuffix

	f, err := os.Open(statePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()

	state := &resumeState{}

	// Read version (1 byte)
	var version uint8
	if err := binary.Read(f, binary.BigEndian, &version); err != nil {
		return nil, fmt.Errorf("failed to read version: %w", err)
	}
	if version != resumeFileVersion {
		return nil, fmt.Errorf("unsupported resume file version: %d", version)
	}

	// Read source length and source string
	var sourceLen uint32
	if err := binary.Read(f, binary.BigEndian, &sourceLen); err != nil {
		return nil, fmt.Errorf("failed to read source length: %w", err)
	}
	sourceBytes := make([]byte, sourceLen)
	if _, err := io.ReadFull(f, sourceBytes); err != nil {
		return nil, fmt.Errorf("failed to read source: %w", err)
	}
	state.Source = string(sourceBytes)

	// Read destination length and destination string
	var destLen uint32
	if err := binary.Read(f, binary.BigEndian, &destLen); err != nil {
		return nil, fmt.Errorf("failed to read destination length: %w", err)
	}
	destBytes := make([]byte, destLen)
	if _, err := io.ReadFull(f, destBytes); err != nil {
		return nil, fmt.Errorf("failed to read destination: %w", err)
	}
	state.Destination = string(destBytes)

	// Read size
	if err := binary.Read(f, binary.BigEndian, &state.Size); err != nil {
		return nil, fmt.Errorf("failed to read size: %w", err)
	}

	// Read chunk size
	if err := binary.Read(f, binary.BigEndian, &state.ChunkSize); err != nil {
		return nil, fmt.Errorf("failed to read chunk size: %w", err)
	}

	// Read num chunks (read as int32 then convert to int)
	var numChunks int32
	if err := binary.Read(f, binary.BigEndian, &numChunks); err != nil {
		return nil, fmt.Errorf("failed to read num chunks: %w", err)
	}
	state.NumChunks = int(numChunks)

	// Read ETag length and ETag string
	var etagLen uint32
	if err := binary.Read(f, binary.BigEndian, &etagLen); err != nil {
		return nil, fmt.Errorf("failed to read etag length: %w", err)
	}
	if etagLen > 0 {
		etagBytes := make([]byte, etagLen)
		if _, err := io.ReadFull(f, etagBytes); err != nil {
			return nil, fmt.Errorf("failed to read etag: %w", err)
		}
		state.ETag = string(etagBytes)
	}

	// Read ModTime (Unix timestamp in seconds)
	var modTimeSec int64
	if err := binary.Read(f, binary.BigEndian, &modTimeSec); err != nil {
		return nil, fmt.Errorf("failed to read modtime: %w", err)
	}
	state.ModTime = time.Unix(modTimeSec, 0)

	// Read bitmap
	bitmapLen := (state.NumChunks + 7) / 8
	state.Bitmap = make([]byte, bitmapLen)
	if _, err := io.ReadFull(f, state.Bitmap); err != nil {
		return nil, fmt.Errorf("failed to read bitmap: %w", err)
	}

	return state, nil
}

// save saves the resume state to binary file
func (rs *resumeState) save(filePath string) error {
	statePath := filePath + resumeStateSuffix
	tempPath := statePath + ".tmp"

	f, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}

	// Write version
	if err := binary.Write(f, binary.BigEndian, uint8(resumeFileVersion)); err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to write version: %w", err)
	}

	// Write source
	if err := binary.Write(f, binary.BigEndian, uint32(len(rs.Source))); err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to write source length: %w", err)
	}
	if _, err := f.WriteString(rs.Source); err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to write source: %w", err)
	}

	// Write destination
	if err := binary.Write(f, binary.BigEndian, uint32(len(rs.Destination))); err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to write destination length: %w", err)
	}
	if _, err := f.WriteString(rs.Destination); err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to write destination: %w", err)
	}

	// Write size
	if err := binary.Write(f, binary.BigEndian, rs.Size); err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to write size: %w", err)
	}

	// Write chunk size
	if err := binary.Write(f, binary.BigEndian, rs.ChunkSize); err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to write chunk size: %w", err)
	}

	// Write num chunks (cast to int32 for fixed size)
	if err := binary.Write(f, binary.BigEndian, int32(rs.NumChunks)); err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to write num chunks: %w", err)
	}

	// Write ETag
	if err := binary.Write(f, binary.BigEndian, uint32(len(rs.ETag))); err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to write etag length: %w", err)
	}
	if _, err := f.WriteString(rs.ETag); err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to write etag: %w", err)
	}

	// Write ModTime (Unix timestamp)
	if err := binary.Write(f, binary.BigEndian, rs.ModTime.Unix()); err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to write modtime: %w", err)
	}

	// Write bitmap
	if _, err := f.Write(rs.Bitmap); err != nil {
		f.Close()
		os.Remove(tempPath)
		return fmt.Errorf("failed to write bitmap: %w", err)
	}

	if err := f.Close(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to close file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, statePath); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename file: %w", err)
	}

	return nil
}

// deleteResumeState deletes the resume state file
func deleteResumeState(filePath string) error {
	statePath := filePath + resumeStateSuffix
	if err := os.Remove(statePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete resume state: %w", err)
	}
	return nil
}

// isChunkComplete checks if a chunk is marked as complete
func (rs *resumeState) isChunkComplete(chunkIdx int) bool {
	if chunkIdx < 0 || chunkIdx >= rs.NumChunks {
		return false
	}
	byteIdx := chunkIdx / 8
	bitIdx := uint(chunkIdx % 8)
	return (rs.Bitmap[byteIdx] & (1 << (7 - bitIdx))) != 0
}

// markChunkComplete marks a chunk as complete
func (rs *resumeState) markChunkComplete(chunkIdx int) {
	if chunkIdx < 0 || chunkIdx >= rs.NumChunks {
		return
	}
	byteIdx := chunkIdx / 8
	bitIdx := uint(chunkIdx % 8)
	rs.Bitmap[byteIdx] |= 1 << (7 - bitIdx)
}

// countComplete returns the number of complete chunks
func (rs *resumeState) countComplete() int {
	count := 0
	for i := 0; i < rs.NumChunks; i++ {
		if rs.isChunkComplete(i) {
			count++
		}
	}
	return count
}

// validate checks if the resume state is valid for the given parameters
func (rs *resumeState) validate(source string, size int64, chunkSize int64) error {
	if rs.Source != source {
		return fmt.Errorf("source mismatch: expected %s, got %s", rs.Source, source)
	}
	if rs.Size != size {
		return fmt.Errorf("size mismatch: expected %d, got %d", rs.Size, size)
	}
	if rs.ChunkSize != chunkSize {
		return fmt.Errorf("chunk size mismatch: expected %d, got %d", rs.ChunkSize, chunkSize)
	}
	return nil
}

// newResumeState creates a new resume state with empty bitmap
func newResumeState(source, destination string, size, chunkSize int64) *resumeState {
	numChunks := calculateNumChunks(size, chunkSize)
	bitmapLen := (numChunks + 7) / 8
	return &resumeState{
		Source:      source,
		Destination: destination,
		Size:        size,
		ChunkSize:   chunkSize,
		NumChunks:   numChunks,
		Bitmap:      make([]byte, bitmapLen),
	}
}

// Return a boolean as to whether we should use multi thread copy for
// this transfer
func doMultiThreadCopy(ctx context.Context, f fs.Fs, src fs.Object) bool {
	ci := fs.GetConfig(ctx)

	// Disable multi thread if...

	// ...it isn't configured
	if ci.MultiThreadStreams <= 1 {
		return false
	}
	// ...if the source doesn't support it
	if src.Fs().Features().NoMultiThreading {
		return false
	}
	// ...size of object is less than cutoff
	if src.Size() < int64(ci.MultiThreadCutoff) {
		return false
	}
	// ...destination doesn't support it
	dstFeatures := f.Features()
	if dstFeatures.OpenChunkWriter == nil && dstFeatures.OpenWriterAt == nil {
		return false
	}
	// ...if --multi-thread-streams not in use and source and
	// destination are both local
	if !ci.MultiThreadSet && dstFeatures.IsLocal && src.Fs().Features().IsLocal {
		return false
	}
	return true
}

// state for a multi-thread copy
type multiThreadCopyState struct {
	ctx         context.Context
	partSize    int64
	size        int64
	src         fs.Object
	acc         *accounting.Account
	numChunks   int
	noBuffering bool // set to read the input without buffering
}

// Copy a single chunk into place
func (mc *multiThreadCopyState) copyChunk(ctx context.Context, chunk int, writer fs.ChunkWriter) (err error) {
	defer func() {
		if err != nil {
			fs.Debugf(mc.src, "multi-thread copy: chunk %d/%d failed: %v", chunk+1, mc.numChunks, err)
		}
	}()
	start := int64(chunk) * mc.partSize
	if start >= mc.size {
		return nil
	}
	end := min(start+mc.partSize, mc.size)
	size := end - start

	// Reserve the memory first so we don't open the source and wait for memory buffers for ages
	var rw *pool.RW
	if !mc.noBuffering {
		rw = multipart.NewRW().Reserve(size)
		defer fs.CheckClose(rw, &err)
	}

	fs.Debugf(mc.src, "multi-thread copy: chunk %d/%d (%d-%d) size %v starting", chunk+1, mc.numChunks, start, end, fs.SizeSuffix(size))

	rc, err := Open(ctx, mc.src, &fs.RangeOption{Start: start, End: end - 1})
	if err != nil {
		return fmt.Errorf("multi-thread copy: failed to open source: %w", err)
	}
	defer fs.CheckClose(rc, &err)

	var rs io.ReadSeeker

	if mc.noBuffering {
		// Read directly if we are sure we aren't going to seek
		// and account with accounting
		rc.SetAccounting(mc.acc.AccountRead)
		rs = rc
	} else {
		// Read the chunk into buffered reader
		_, err = io.CopyN(rw, rc, size)
		if err != nil {
			return fmt.Errorf("multi-thread copy: failed to read chunk: %w", err)
		}
		// Account as we go
		rw.SetAccounting(mc.acc.AccountRead)
		rs = rw
	}

	// Write the chunk
	bytesWritten, err := writer.WriteChunk(ctx, chunk, rs)
	if err != nil {
		return fmt.Errorf("multi-thread copy: failed to write chunk: %w", err)
	}

	fs.Debugf(mc.src, "multi-thread copy: chunk %d/%d (%d-%d) size %v finished", chunk+1, mc.numChunks, start, end, fs.SizeSuffix(bytesWritten))
	return nil
}

// Given a file size and a chunkSize
// it returns the number of chunks, so that chunkSize * numChunks >= size
func calculateNumChunks(size int64, chunkSize int64) int {
	numChunks := size / chunkSize
	if size%chunkSize != 0 {
		numChunks++
	}
	return int(numChunks)
}

// Copy src to (f, remote) using streams download threads. It tries to use the OpenChunkWriter feature
// and if that's not available it creates an adapter using OpenWriterAt
func multiThreadCopy(ctx context.Context, f fs.Fs, remote string, src fs.Object, concurrency int, tr *accounting.Transfer, options ...fs.OpenOption) (newDst fs.Object, err error) {
	openChunkWriter := f.Features().OpenChunkWriter
	ci := fs.GetConfig(ctx)

	// Check if resume is enabled and destination supports it
	resumeEnabled := ci.MultiThreadResume && f.Features().IsLocal

	var state *resumeState
	var localPath string

	noBuffering := false
	usingOpenWriterAt := false
	if openChunkWriter == nil {
		openWriterAt := f.Features().OpenWriterAt
		if openWriterAt == nil {
			return nil, errors.New("multi-thread copy: neither OpenChunkWriter nor OpenWriterAt supported")
		}
		openChunkWriter = openChunkWriterFromOpenWriterAt(openWriterAt, int64(ci.MultiThreadChunkSize), int64(ci.MultiThreadWriteBufferSize), f)
		// If we are using OpenWriterAt we don't seek the chunks so don't need to buffer
		fs.Debugf(src, "multi-thread copy: disabling buffering because destination uses OpenWriterAt")
		noBuffering = true
		usingOpenWriterAt = true
	} else if src.Fs().Features().IsLocal {
		// If the source fs is local we don't need to buffer
		fs.Debugf(src, "multi-thread copy: disabling buffering because source is local disk")
		noBuffering = true
	} else if f.Features().ChunkWriterDoesntSeek {
		// If the destination Fs promises not to seek its chunks
		// (except for retries) then we don't need buffering.
		fs.Debugf(src, "multi-thread copy: disabling buffering because destination has set ChunkWriterDoesntSeek")
		noBuffering = true
	}

	if src.Size() < 0 {
		return nil, fmt.Errorf("multi-thread copy: can't copy unknown sized file")
	}
	if src.Size() == 0 {
		return nil, fmt.Errorf("multi-thread copy: can't copy zero sized file")
	}

	info, chunkWriter, err := openChunkWriter(ctx, remote, src, options...)
	if err != nil {
		return nil, fmt.Errorf("multi-thread copy: failed to open chunk writer: %w", err)
	}

	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	uploadedOK := false
	defer atexit.OnError(&err, func() {
		cancel()
		if info.LeavePartsOnError || uploadedOK || resumeEnabled {
			return
		}
		fs.Debugf(src, "multi-thread copy: cancelling transfer on exit")
		abortErr := chunkWriter.Abort(ctx)
		if abortErr != nil {
			fs.Debugf(src, "multi-thread copy: abort failed: %v", abortErr)
		}
	})()

	if info.ChunkSize > src.Size() {
		fs.Debugf(src, "multi-thread copy: chunk size %v was bigger than source file size %v", fs.SizeSuffix(info.ChunkSize), fs.SizeSuffix(src.Size()))
		info.ChunkSize = src.Size()
	}

	// Use the backend concurrency if it is higher than --multi-thread-streams or if --multi-thread-streams wasn't set explicitly
	if !ci.MultiThreadSet || info.Concurrency > concurrency {
		fs.Debugf(src, "multi-thread copy: using backend concurrency of %d instead of --multi-thread-streams %d", info.Concurrency, concurrency)
		concurrency = info.Concurrency
	}

	numChunks := calculateNumChunks(src.Size(), info.ChunkSize)
	if concurrency > numChunks {
		fs.Debugf(src, "multi-thread copy: number of streams %d was bigger than number of chunks %d", concurrency, numChunks)
		concurrency = numChunks
	}

	if concurrency < 1 {
		concurrency = 1
	}

	// Initialize or validate resume state
	if resumeEnabled {
		localPath = filepath.Join(f.Root(), remote)

		// Try to load existing resume state
		state, err = loadResumeState(localPath)
		if err != nil {
			fs.Debugf(remote, "Failed to load resume state, starting fresh: %v", err)
			state = nil
		} else if state != nil {
			// Validate state against current file
			if err := state.validate(src.Remote(), src.Size(), info.ChunkSize); err != nil {
				fs.Debugf(remote, "Resume state invalid (%v), starting fresh", err)
				state = nil
			} else {
				// Validate ETag if available
				currentETag, err := src.Hash(ctx, hash.MD5)
				if err == nil && currentETag != "" && state.ETag != "" && currentETag != state.ETag {
					fs.Logf(remote, "ETag mismatch (source changed), starting fresh download")
					state = nil
				} else if !state.ModTime.IsZero() {
					currentModTime := src.ModTime(ctx)
					if !currentModTime.Equal(state.ModTime) {
						fs.Logf(remote, "Modification time mismatch (source changed), starting fresh download")
						state = nil
					}
				}
			}
		}

		if state == nil {
			state = newResumeState(src.Remote(), localPath, src.Size(), info.ChunkSize)
			// Store ETag and ModTime
			etag, err := src.Hash(ctx, hash.MD5)
			if err == nil {
				state.ETag = etag
			}
			state.ModTime = src.ModTime(ctx)
		}

		completed := state.countComplete()
		if completed > 0 {
			fs.Logf(remote, "Resuming download: %d/%d chunks already complete", completed, state.NumChunks)
		}
	}

	g, gCtx := errgroup.WithContext(uploadCtx)
	g.SetLimit(concurrency)

	mc := &multiThreadCopyState{
		ctx:         gCtx,
		size:        src.Size(),
		src:         src,
		partSize:    info.ChunkSize,
		numChunks:   numChunks,
		noBuffering: noBuffering,
	}

	// Make accounting
	mc.acc = tr.Account(gCtx, nil)

	// Account for already-downloaded bytes to show correct progress position
	if resumeEnabled && state != nil {
		var completedBytes int64
		for chunkIdx := range numChunks {
			if state.isChunkComplete(chunkIdx) {
				chunkStart := int64(chunkIdx) * info.ChunkSize
				chunkEnd := min(chunkStart+info.ChunkSize, src.Size())
				completedBytes += chunkEnd - chunkStart
			}
		}
		if completedBytes > 0 {
			mc.acc.AccountReadN(completedBytes)
		}
	}

	fs.Debugf(src, "Starting multi-thread copy with %d chunks of size %v with %v parallel streams", mc.numChunks, fs.SizeSuffix(mc.partSize), concurrency)

	// Track chunks for state saving
	var stateMu sync.Mutex

	for chunk := range mc.numChunks {
		// Skip completed chunks if resuming
		if resumeEnabled && state != nil && state.isChunkComplete(chunk) {
			continue
		}

		// Fail fast, in case an errgroup managed function returns an error
		if gCtx.Err() != nil {
			break
		}
		chunk := chunk
		g.Go(func() error {
			err := mc.copyChunk(gCtx, chunk, chunkWriter)
			if err != nil {
				return err
			}

			// Mark as complete and save state (only after successful write)
			if resumeEnabled && state != nil {
				stateMu.Lock()
				state.markChunkComplete(chunk)
				if localPath != "" {
					if saveErr := state.save(localPath); saveErr != nil {
						fs.Debugf(remote, "Failed to save resume state: %v", saveErr)
					}
				}
				stateMu.Unlock()
			}
			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		return nil, err
	}
	err = chunkWriter.Close(ctx)
	if err != nil {
		return nil, fmt.Errorf("multi-thread copy: failed to close object after copy: %w", err)
	}
	uploadedOK = true // file is definitely uploaded OK so no need to abort

	// Delete state file on successful completion
	if resumeEnabled && localPath != "" {
		if err := deleteResumeState(localPath); err != nil {
			fs.Debugf(remote, "Failed to delete resume state: %v", err)
		}
	}

	obj, err := f.NewObject(ctx, remote)
	if err != nil {
		return nil, fmt.Errorf("multi-thread copy: failed to find object after copy: %w", err)
	}

	// OpenWriterAt doesn't set metadata so we need to set it on completion
	if usingOpenWriterAt {
		setModTime := true
		if ci.Metadata {
			do, ok := obj.(fs.SetMetadataer)
			if ok {
				meta, err := fs.GetMetadataOptions(ctx, f, src, options)
				if err != nil {
					return nil, fmt.Errorf("multi-thread copy: failed to read metadata from source object: %w", err)
				}
				if _, foundMeta := meta["mtime"]; !foundMeta {
					meta.Set("mtime", src.ModTime(ctx).Format(time.RFC3339Nano))
				}
				err = do.SetMetadata(ctx, meta)
				if err != nil {
					return nil, fmt.Errorf("multi-thread copy: failed to set metadata: %w", err)
				}
				setModTime = false
			} else {
				fs.Errorf(obj, "multi-thread copy: can't set metadata as SetMetadata isn't implemented in: %v", f)
			}
		}
		if setModTime {
			err = obj.SetModTime(ctx, src.ModTime(ctx))
			switch err {
			case nil, fs.ErrorCantSetModTime, fs.ErrorCantSetModTimeWithoutDelete:
			default:
				return nil, fmt.Errorf("multi-thread copy: failed to set modification time: %w", err)
			}
		}
	}

	fs.Debugf(src, "Finished multi-thread copy with %d parts of size %v", mc.numChunks, fs.SizeSuffix(mc.partSize))
	return obj, nil
}

// writerAtChunkWriter converts a WriterAtCloser into a ChunkWriter
type writerAtChunkWriter struct {
	remote          string
	size            int64
	writerAt        fs.WriterAtCloser
	chunkSize       int64
	chunks          int
	writeBufferSize int64
	f               fs.Fs
	closed          bool
}

// WriteChunk writes chunkNumber from reader
func (w *writerAtChunkWriter) WriteChunk(ctx context.Context, chunkNumber int, reader io.ReadSeeker) (int64, error) {
	fs.Debugf(w.remote, "writing chunk %v", chunkNumber)

	bytesToWrite := w.chunkSize
	if chunkNumber == (w.chunks-1) && w.size%w.chunkSize != 0 {
		bytesToWrite = w.size % w.chunkSize
	}

	var writer io.Writer = io.NewOffsetWriter(w.writerAt, int64(chunkNumber)*w.chunkSize)
	if w.writeBufferSize > 0 {
		writer = bufio.NewWriterSize(writer, int(w.writeBufferSize))
	}
	n, err := io.Copy(writer, reader)
	if err != nil {
		return -1, err
	}
	if n != bytesToWrite {
		return -1, fmt.Errorf("expected to write %v bytes for chunk %v, but wrote %v bytes", bytesToWrite, chunkNumber, n)
	}
	// if we were buffering, flush to disk
	switch w := writer.(type) {
	case *bufio.Writer:
		err = w.Flush()
		if err != nil {
			return -1, fmt.Errorf("multi-thread copy: flush failed: %w", err)
		}
	}
	return n, nil
}

// Close the chunk writing
func (w *writerAtChunkWriter) Close(ctx context.Context) error {
	if w.closed {
		return nil
	}
	w.closed = true
	return w.writerAt.Close()
}

// Abort the chunk writing
func (w *writerAtChunkWriter) Abort(ctx context.Context) error {
	err := w.Close(ctx)
	if err != nil {
		fs.Errorf(w.remote, "multi-thread copy: failed to close file before aborting: %v", err)
	}
	obj, err := w.f.NewObject(ctx, w.remote)
	if err != nil {
		return fmt.Errorf("multi-thread copy: failed to find temp file when aborting chunk writer: %w", err)
	}
	return obj.Remove(ctx)
}

// openChunkWriterFromOpenWriterAt adapts an OpenWriterAtFn into an OpenChunkWriterFn using chunkSize and writeBufferSize
func openChunkWriterFromOpenWriterAt(openWriterAt fs.OpenWriterAtFn, chunkSize int64, writeBufferSize int64, f fs.Fs) fs.OpenChunkWriterFn {
	return func(ctx context.Context, remote string, src fs.ObjectInfo, options ...fs.OpenOption) (info fs.ChunkWriterInfo, writer fs.ChunkWriter, err error) {
		ci := fs.GetConfig(ctx)

		writerAt, err := openWriterAt(ctx, remote, src.Size())
		if err != nil {
			return info, nil, err
		}

		if writeBufferSize > 0 {
			fs.Debugf(src.Remote(), "multi-thread copy: write buffer set to %v", writeBufferSize)
		}

		chunkWriter := &writerAtChunkWriter{
			remote:          remote,
			size:            src.Size(),
			chunkSize:       chunkSize,
			chunks:          calculateNumChunks(src.Size(), chunkSize),
			writerAt:        writerAt,
			writeBufferSize: writeBufferSize,
			f:               f,
		}
		info = fs.ChunkWriterInfo{
			ChunkSize:   chunkSize,
			Concurrency: ci.MultiThreadStreams,
		}
		return info, chunkWriter, nil
	}
}
