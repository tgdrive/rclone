// Package teldrive provides an interface to the teldrive storage system.
package teldrive

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/teldrive/api"
	"github.com/rclone/rclone/backend/teldrive/tdhash"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/lib/dircache"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
	"golang.org/x/sync/errgroup"
)

const (
	timeFormat       = time.RFC3339
	maxChunkSize     = 2000 * fs.Mebi // 125 × 16MB (Telegram limit)
	defaultChunkSize = 512 * fs.Mebi  // 32 × 16MB for optimal BLAKE3 tree hashing
	minChunkSize     = 64 * fs.Mebi   // 4 × 16MB
	authCookieName   = "access_token"
)

var telDriveHash hash.Type

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "teldrive",
		Description: "Tel Drive",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Help:      "Access Token Cookie",
			Name:      "access_token",
			Sensitive: true,
		}, {
			Help:      "Api Host",
			Name:      "api_host",
			Sensitive: true,
		}, {
			Help:    "Chunk Size",
			Name:    "chunk_size",
			Default: defaultChunkSize,
		}, {
			Help:    "Page Size for listing files",
			Name:    "page_size",
			Default: 500,
		}, {
			Name:     "random_chunk_name",
			Default:  true,
			Help:     "Random Names For Chunks for Security",
			Advanced: true,
		}, {
			Name:      "channel_id",
			Help:      "Channel ID",
			Sensitive: true,
		}, {
			Name:     "upload_concurrency",
			Default:  4,
			Help:     "Upload Concurrency",
			Advanced: true,
		},
			{
				Name:     "threaded_streams",
				Default:  false,
				Help:     "Thread Streams",
				Advanced: true,
			},
			{
				Help:      "Upload Api Host",
				Name:      "upload_host",
				Sensitive: true,
			},
			{
				Name:    "encrypt_files",
				Default: false,
				Help:    "Enable Native Teldrive Encryption",
			},
			{
				Name:    "hash_enabled",
				Default: true,
				Help:    "Enable Blake3 Tree Hashing",
			},
			{
				Name:     config.ConfigEncoding,
				Help:     config.ConfigEncodingHelp,
				Advanced: true,
				Default:  encoder.Standard | encoder.EncodeInvalidUtf8,
			}},
	})

	telDriveHash = hash.RegisterHash("teldrive", "TelDriveHash", tdhash.Size, tdhash.New)
}

// Options defines the configuration for this backend
type Options struct {
	ApiHost           string               `config:"api_host"`
	UploadHost        string               `config:"upload_host"`
	AccessToken       string               `config:"access_token"`
	ChunkSize         fs.SizeSuffix        `config:"chunk_size"`
	RootFolderID      string               `config:"root_folder_id"`
	RandomChunkName   bool                 `config:"random_chunk_name"`
	UploadConcurrency int                  `config:"upload_concurrency"`
	ChannelID         int64                `config:"channel_id"`
	EncryptFiles      bool                 `config:"encrypt_files"`
	PageSize          int64                `config:"page_size"`
	HashEnabled       bool                 `config:"hash_enabled"`
	Enc               encoder.MultiEncoder `config:"encoding"`
}

// Fs is the interface a cloud storage system must provide
type Fs struct {
	root         string
	name         string
	opt          Options
	features     *fs.Features
	srv          *rest.Client
	pacer        *fs.Pacer
	ssePacer     *fs.Pacer // Dedicated pacer for SSE connection retries
	userId       int64
	dirCache     *dircache.DirCache
	rootFolderID string
}

// Object represents an teldrive object
type Object struct {
	fs       *Fs
	remote   string
	id       string
	size     int64
	parentId string
	name     string
	modTime  time.Time
	mimeType string
	hash     string // BLAKE3 tree hash from server
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// String returns a description of the FS
func (f *Fs) String() string {
	return fmt.Sprintf("teldrive root '%s'", f.root)
}

// Precision of the ModTimes in this Fs
func (f *Fs) Precision() time.Duration {
	return time.Second
}

// Hashes returns the supported hash types of the filesystem
// TelDrive uses BLAKE3 tree hashing only (16MB fixed blocks)
func (f *Fs) Hashes() hash.Set {
	if f.opt.HashEnabled {
		return hash.Set(telDriveHash)
	}
	return hash.NewHashSet(hash.None)

}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// retryErrorCodes is a slice of error codes that we will retry
var retryErrorCodes = []int{
	429, // Too Many Requests.
	500, // Internal Server Error
	502, // Bad Gateway
	503, // Service Unavailable
	504, // Gateway Timeout
	509, // Bandwidth Limit Exceeded
}

// shouldRetry returns a boolean as to whether this resp and err
// deserve to be retried.  It returns the err as a convenience
func shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

// alignChunkSize rounds the chunk size to the nearest 16MB multiple
// and clamps it to min/max bounds
func alignChunkSize(cs fs.SizeSuffix) fs.SizeSuffix {
	blockSize := int64(16 * 1024 * 1024) // 16MB
	chunkSizeBytes := min(max(int64(cs), int64(minChunkSize)), int64(maxChunkSize))
	// Round to nearest 16MB multiple
	// Ensure we don't exceed max after rounding
	alignedSize := min(((chunkSizeBytes+blockSize/2)/blockSize)*blockSize, int64(maxChunkSize))

	return fs.SizeSuffix(alignedSize)
}

func Ptr[T any](t T) *T {
	return &t
}

// NewFs makes a new Fs object from the path
//
// The path is of the form remote:path
//
// Remotes are looked up in the config file.  If the remote isn't
// found then NotFoundInConfigFile will be returned.
//
// On Windows avoid single character remote names as they can be mixed
// up with drive letters.
func NewFs(ctx context.Context, name string, root string, config configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(config, opt)
	if err != nil {
		return nil, err
	}

	// Align chunk size to 16MB multiple for optimal BLAKE3 tree hashing
	opt.ChunkSize = alignChunkSize(opt.ChunkSize)

	if opt.ChannelID < 0 {
		channelIDStr := strconv.FormatInt(opt.ChannelID, 10)
		// teldrive API expects channel ID without the -100 prefix for supergroups/channels
		trimmedIDStr := strings.TrimPrefix(channelIDStr, "-100")
		newID, err := strconv.ParseInt(trimmedIDStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid channel_id: %w", err)
		}
		opt.ChannelID = newID
	}

	f := &Fs{
		name:  name,
		root:  root,
		opt:   *opt,
		pacer: fs.NewPacer(ctx, pacer.NewDefault()),
		// Dedicated SSE pacer with optimized settings for connection retries
		ssePacer: fs.NewPacer(ctx, pacer.NewDefault(
			pacer.MinSleep(1*time.Second),
			pacer.MaxSleep(30*time.Second),
			pacer.DecayConstant(2),
		)),
	}

	f.root = strings.Trim(root, "/")

	f.features = (&fs.Features{
		CanHaveEmptyDirectories: true,
		ReadMimeType:            true,
		ChunkWriterDoesntSeek:   true,
	}).Fill(ctx, f)

	client := fshttp.NewClient(ctx)
	authCookie := http.Cookie{Name: authCookieName, Value: opt.AccessToken}
	f.srv = rest.NewClient(client).SetRoot(strings.Trim(opt.ApiHost, "/")).SetCookie(&authCookie)

	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/auth/session",
	}

	var (
		session     api.Session
		sessionResp *http.Response
	)

	err = f.pacer.Call(func() (bool, error) {
		sessionResp, err = f.srv.CallJSON(ctx, &opts, nil, &session)
		return shouldRetry(ctx, sessionResp, err)
	})

	if err != nil {
		return nil, err
	}
	if session.UserId == 0 {
		return nil, errors.New("invalid session")
	}

	for _, cookie := range sessionResp.Cookies() {
		if (cookie.Name == authCookieName) && (cookie.Value != "") {
			config.Set(authCookieName, cookie.Value)
		}
	}

	f.userId = session.UserId

	if f.opt.RootFolderID != "" {
		f.rootFolderID = f.opt.RootFolderID
	} else {
		f.rootFolderID, err = f.getRootID(ctx)
		if err != nil {
			return nil, err
		}
		config.Set("root_folder_id", f.rootFolderID)
	}
	f.dirCache = dircache.New(f.root, f.rootFolderID, f)
	err = f.dirCache.FindRoot(ctx, false)
	if err != nil {
		// Assume it is a file
		newRoot, remote := dircache.SplitPath(root)
		tempF := *f
		tempF.dirCache = dircache.New(newRoot, f.rootFolderID, &tempF)
		tempF.root = newRoot
		err = tempF.dirCache.FindRoot(ctx, false)
		if err != nil {
			// No root so return old f
			return f, nil
		}
		_, err := tempF.NewObject(ctx, remote)
		if err != nil {
			if errors.Is(err, fs.ErrorObjectNotFound) || errors.Is(err, fs.ErrorIsDir) {
				// File doesn't exist so return old f
				return f, nil
			}
			return nil, err
		}
		f.features.Fill(ctx, &tempF)
		// XXX: update the old f here instead of returning tempF, since
		// `features` were already filled with functions having *f as a receiver.
		// See https://github.com/rclone/rclone/issues/2182
		f.dirCache = tempF.dirCache
		f.root = tempF.root
		return f, fs.ErrorIsFile

	}
	return f, nil
}

func (f *Fs) readMetaDataForPath(ctx context.Context, path string, options *api.MetadataRequestOptions) (*api.ReadMetadataResponse, error) {

	directoryID, err := f.dirCache.FindDir(ctx, path, false)

	if err != nil {
		return nil, err
	}
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/files",
		Parameters: url.Values{
			"parentId":  []string{directoryID},
			"limit":     []string{strconv.FormatInt(options.Limit, 10)},
			"sort":      []string{"id"},
			"operation": []string{"list"},
			"page":      []string{strconv.FormatInt(options.Page, 10)},
		},
	}
	var info api.ReadMetadataResponse
	var resp *http.Response

	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, err
	}
	return &info, nil
}

func (f *Fs) getRootID(ctx context.Context) (string, error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/files",
		Parameters: url.Values{
			"parentId":  []string{"nil"},
			"operation": []string{"find"},
			"name":      []string{"root"},
			"type":      []string{"folder"},
		},
	}
	var err error
	var info api.ReadMetadataResponse
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, err)
	})

	if err != nil {
		return "", err
	}
	if len(info.Files) == 0 {
		return "", fmt.Errorf("couldn't find root directory ID: %w", err)
	}
	return info.Files[0].Id, nil
}

func (f *Fs) getFileShare(ctx context.Context, id string) (*api.FileShare, error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/files/" + id + "/share",
	}
	res := api.FileShare{}
	var (
		resp *http.Response
		err  error
	)
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &res)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		if resp.StatusCode == http.StatusNotFound {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}
	if res.ExpiresAt != nil && res.ExpiresAt.UTC().Before(time.Now().UTC()) {
		return nil, fs.ErrorObjectNotFound
	}
	return &res, nil
}

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {

	opts := &api.MetadataRequestOptions{
		Limit: f.opt.PageSize,
		Page:  1,
	}

	files := []api.FileInfo{}

	info, err := f.readMetaDataForPath(ctx, dir, opts)

	if err != nil {
		return nil, err
	}

	files = append(files, info.Files...)
	mu := sync.Mutex{}
	if info.Meta.TotalPages > 1 {
		g, _ := errgroup.WithContext(ctx)

		g.SetLimit(8)

		for i := 2; i <= info.Meta.TotalPages; i++ {
			page := i
			g.Go(func() error {
				opts := &api.MetadataRequestOptions{
					Limit: f.opt.PageSize,
					Page:  int64(page),
				}
				info, err := f.readMetaDataForPath(ctx, dir, opts)
				if err != nil {
					return err
				}
				mu.Lock()
				files = append(files, info.Files...)
				mu.Unlock()
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return nil, err
		}
	}

	for _, item := range files {
		remote := path.Join(dir, f.opt.Enc.ToStandardName(item.Name))
		if item.Type == "folder" {
			f.dirCache.Put(remote, item.Id)
			d := fs.NewDir(remote, item.ModTime).SetID(item.Id).SetParentID(item.ParentId).
				SetSize(item.Size)
			entries = append(entries, d)
		}
		if item.Type == "file" {
			o, err := f.newObjectWithInfo(ctx, remote, &item)
			if err != nil {
				continue
			}
			entries = append(entries, o)
		}

	}
	return entries, nil
}

// Return an Object from a path
//
// If it can't be found it returns the error fs.ErrorObjectNotFound.
func (f *Fs) newObjectWithInfo(_ context.Context, remote string, info *api.FileInfo) (fs.Object, error) {
	if info == nil {
		return nil, fs.ErrorObjectNotFound
	}
	o := &Object{
		fs:       f,
		remote:   remote,
		id:       info.Id,
		size:     info.Size,
		parentId: info.ParentId,
		name:     info.Name,
		modTime:  info.ModTime,
		mimeType: info.MimeType,
		hash:     info.Hash,
	}
	if info.Type == "folder" {
		return o, fs.ErrorIsDir
	}
	return o, nil
}

// NewObject finds the Object at remote.  If it can't be found it
// returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	leaf, directoryID, err := f.dirCache.FindPath(ctx, remote, false)
	if err != nil {
		if err == fs.ErrorDirNotFound {
			return nil, fs.ErrorObjectNotFound
		}
	}

	res, err := f.findObject(ctx, directoryID, leaf)
	if err != nil || len(res) == 0 {
		return nil, fs.ErrorObjectNotFound
	}
	if res[0].Type == "folder" {
		return nil, fs.ErrorIsDir
	}

	return f.newObjectWithInfo(ctx, remote, &res[0])
}

func (f *Fs) findObject(ctx context.Context, pathID, leaf string) ([]api.FileInfo, error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/files",
		Parameters: url.Values{
			"parentId":  []string{pathID},
			"operation": []string{"find"},
			"name":      []string{leaf},
			"sort":      []string{"id"},
			"order":     []string{"desc"},
			"limit":     []string{"1"},
		},
	}
	var info api.ReadMetadataResponse
	err := f.pacer.Call(func() (bool, error) {
		resp, err := f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, err
	}
	return info.Files, nil
}

func (f *Fs) moveTo(ctx context.Context, id, srcLeaf, dstLeaf, srcDirectoryID, dstDirectoryID string) error {

	if srcDirectoryID != dstDirectoryID {
		opts := rest.Opts{
			Method:     "POST",
			Path:       "/api/files/move",
			NoResponse: true,
		}
		mv := api.MoveFileRequest{
			Destination:     dstDirectoryID,
			Files:           []string{id},
			DestinationLeaf: dstLeaf,
		}
		err := f.pacer.Call(func() (bool, error) {
			resp, err := f.srv.CallJSON(ctx, &opts, &mv, nil)
			return shouldRetry(ctx, resp, err)
		})
		if err != nil {
			return fmt.Errorf("couldn't move file: %w", err)
		}
	} else {
		if srcLeaf != dstLeaf {
			err := f.updateFileInformation(ctx, &api.UpdateFileInformation{Name: dstLeaf}, id)
			if err != nil {
				return fmt.Errorf("move: failed rename: %w", err)
			}
		}
	}

	return nil
}

// updateFileInformation set's various file attributes most importantly it's name
func (f *Fs) updateFileInformation(ctx context.Context, update *api.UpdateFileInformation, fileId string) (err error) {
	opts := rest.Opts{
		Method:     "PATCH",
		Path:       "/api/files/" + fileId,
		NoResponse: true,
	}

	err = f.pacer.Call(func() (bool, error) {
		resp, err := f.srv.CallJSON(ctx, &opts, update, nil)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return fmt.Errorf("couldn't update file info: %w", err)
	}
	return err
}

func (f *Fs) putUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, _ ...fs.OpenOption) error {
	o := &Object{
		fs: f,
	}

	var uploadInfo *uploadInfo
	var err error
	size := src.Size()

	if size < 0 {
		// Unknown size - buffer to memory/temp file first
		fs.Debugf(f, "putUnchecked: unknown size, buffering to memory (threshold: %d bytes)", memoryBufferThreshold)
		uploadInfo, size, err = o.uploadWithBuffering(ctx, in, src)
		if err != nil {
			return err
		}
		// Create new src with known size for createFile
		src = object.NewStaticObjectInfo(src.Remote(), src.ModTime(ctx), size, false, nil, f)
	} else if size > 0 {
		uploadInfo, err = o.uploadMultipart(ctx, in, src)
		if err != nil {
			return err
		}
	}

	return o.createFile(ctx, src, uploadInfo)
}

// FindLeaf finds a directory of name leaf in the folder with ID pathID
func (f *Fs) FindLeaf(ctx context.Context, pathID, leaf string) (pathIDOut string, found bool, err error) {
	files, err := f.findObject(ctx, pathID, leaf)
	if err != nil {
		return "", false, err
	}
	if len(files) == 0 {
		return "", false, nil
	}
	if files[0].Type == "file" {
		return "", false, fs.ErrorIsFile
	}
	return files[0].Id, true, nil
}

// Put in to the remote path with the modTime given of the given size
//
// When called from outside an Fs by rclone, src.Size() will always be >= 0.
// But for unknown-sized objects (indicated by src.Size() == -1), Put should either
// return an error or upload it properly (rather than e.g. calling panic).
//
// May create the object even if it returns an error - if so
// will return the object and the error, otherwise will return
// nil and the error
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	existingObj, err := f.NewObject(ctx, src.Remote())
	switch err {
	case nil:
		return existingObj, existingObj.Update(ctx, in, src, options...)
	case fs.ErrorObjectNotFound:
		// Not found so create it
		return f.PutUnchecked(ctx, in, src, options...)
	default:
		return nil, err
	}
}

// PutUnchecked uploads the object
//
// This will create a duplicate if we upload a new file without
// checking to see if there is one already - use Put() for that.
func (f *Fs) PutUnchecked(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	err := f.putUnchecked(ctx, in, src, options...)
	if err != nil {
		return nil, err
	}
	return f.NewObject(ctx, src.Remote())
}

// Update the already existing object
//
// Copy the reader into the object updating modTime and size.
//
// The new object may have been created if an error is returned
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	remote := o.Remote()
	modTime := src.ModTime(ctx)

	leaf, directoryID, err := o.fs.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return err
	}

	var uploadInfo *uploadInfo
	size := src.Size()

	if size < 0 {
		// Unknown size - buffer to memory/temp file first
		fs.Debugf(o, "Update: unknown size, buffering to memory (threshold: %d bytes)", memoryBufferThreshold)
		uploadInfo, size, err = o.uploadWithBuffering(ctx, in, src)
		if err != nil {
			return err
		}
	} else if size > 0 {
		uploadInfo, err = o.uploadMultipart(ctx, in, src)
		if err != nil {
			return err
		}
	}

	payload := &api.UpdateFileInformation{
		ModTime:  Ptr(modTime.UTC()),
		Size:     size,
		ParentID: directoryID,
		Name:     leaf,
	}

	if uploadInfo != nil {
		payload.UploadId = uploadInfo.uploadID
		payload.ChannelID = uploadInfo.channelID
		payload.Encrypted = uploadInfo.encryptFile
	}

	opts := rest.Opts{
		Method:     "PATCH",
		Path:       "/api/files/" + o.id,
		NoResponse: true,
	}

	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err := o.fs.srv.CallJSON(ctx, &opts, payload, nil)
		return shouldRetry(ctx, resp, err)
	})

	if err != nil {
		return fmt.Errorf("failed to update file information: %w", err)
	}

	o.modTime = modTime
	o.size = size

	return nil
}

// ChangeNotify calls the passed function with a path that has had changes.
func (f *Fs) ChangeNotify(ctx context.Context, notifyFunc func(string, fs.EntryType), pollIntervalChan <-chan time.Duration) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case pollInterval, ok := <-pollIntervalChan:
				if !ok {
					fs.Debugf(f, "ChangeNotify: channel closed, stopping")
					return
				}
				if pollInterval > 0 {
					fs.Debugf(f, "ChangeNotify: poll interval set but SSE is active, ignoring")
				}
			default:
				fs.Debugf(f, "Starting SSE event stream")
				err := f.changeNotifySSE(ctx, notifyFunc)
				if err != nil {
					fs.Infof(f, "SSE connection failed permanently: %s", err)
					return
				}
			}
		}
	}()
}

func isFatalError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "401") ||
		strings.Contains(errStr, "403") ||
		strings.Contains(errStr, "404")
}

func (f *Fs) changeNotifySSE(ctx context.Context, notifyFunc func(string, fs.EntryType)) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var connErr error
		err := f.ssePacer.Call(func() (bool, error) {
			connErr = f.connectAndProcessSSE(ctx, notifyFunc)
			if connErr == nil {
				return false, nil
			}
			if fserrors.ContextError(ctx, &connErr) {
				return false, connErr
			}
			if isFatalError(connErr) {
				return false, connErr
			}
			return true, connErr
		})

		if err != nil {
			return err
		}

		fs.Debugf(f, "SSE connection ended, will retry")
	}
}

func (f *Fs) connectAndProcessSSE(ctx context.Context, notifyFunc func(string, fs.EntryType)) error {
	opts := rest.Opts{
		Method:      "GET",
		Path:        "/api/events/stream",
		ContentType: "text/event-stream",
		ExtraHeaders: map[string]string{
			"Accept":        "text/event-stream",
			"Cache-Control": "no-cache",
		},
	}

	resp, err := f.srv.Call(ctx, &opts)
	if err != nil {
		return fmt.Errorf("failed to connect to SSE endpoint: %w", err)
	}
	if resp == nil || resp.Body == nil {
		return fmt.Errorf("no response from SSE endpoint")
	}
	defer resp.Body.Close()

	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "text/event-stream") {
		return fmt.Errorf("unexpected content type: %s", contentType)
	}

	fs.Debugf(f, "SSE connection established")
	reader := bufio.NewReader(resp.Body)
	var eventData strings.Builder

	for {

		select {
		case <-ctx.Done():
			return nil
		default:
		}

		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("SSE stream closed by server")
			}
			return fmt.Errorf("error reading SSE stream: %w", err)
		}

		line = strings.TrimRight(line, "\r\n")

		if line == "" {
			if eventData.Len() > 0 {
				data := eventData.String()
				eventData.Reset()

				if err := f.processSSEEvent(data, notifyFunc); err != nil {
					fs.Debugf(f, "Failed to process SSE event: %s", err)
				}
			}
			continue
		}

		if strings.HasPrefix(line, "data: ") {
			eventData.WriteString(line[6:])
		}
	}
}

func (f *Fs) processSSEEvent(data string, notifyFunc func(string, fs.EntryType)) error {
	var event api.Event
	if err := json.Unmarshal([]byte(data), &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	// Get parent path from cache
	parentPath, ok := f.dirCache.GetInv(event.Source.ParentId)
	if !ok {
		fs.Debugf(f, "SSE: skipping event for uncached parent %s", event.Source.ParentId)
		return nil
	}

	fullPath := path.Join(parentPath, event.Source.Name)

	// Filter: only notify for paths within root
	if !strings.HasPrefix(fullPath, f.root) {
		return nil
	}

	var entryType fs.EntryType
	switch event.Source.Type {
	case "folder":
		entryType = fs.EntryDirectory
	case "file":
		entryType = fs.EntryObject
	default:
		entryType = fs.EntryObject
	}

	// Handle move events - notify both old and new locations
	if event.Type == "file_move" && event.Source.DestParentId != "" {
		if oldParentPath, ok := f.dirCache.GetInv(event.Source.DestParentId); ok {
			oldPath := path.Join(oldParentPath, event.Source.Name)
			if strings.HasPrefix(oldPath, f.root) {
				fs.Debugf(f, "SSE move event: old path %s", oldPath)
				notifyFunc(oldPath, entryType)
			}
		}
	}

	fs.Debugf(f, "SSE event: %s (%v, type=%s)", fullPath, entryType, event.Type)
	notifyFunc(fullPath, entryType)

	return nil
}

// PutStream uploads to the remote path with the modTime given of indeterminate size
func (f *Fs) PutStream(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return f.Put(ctx, in, src, options...)
}

// OpenChunkWriter returns the chunk size and a ChunkWriter
//
// Pass in the remote and the src object
// You can also use options to hint at the desired chunk size
func (f *Fs) OpenChunkWriter(
	ctx context.Context,
	remote string,
	src fs.ObjectInfo,
	options ...fs.OpenOption) (info fs.ChunkWriterInfo, writer fs.ChunkWriter, err error) {

	o := &Object{
		fs:     f,
		remote: remote,
	}

	// If size is unknown, use bufferingChunkWriter that supports out-of-order chunks
	if src.Size() <= 0 {
		fs.Debugf(f, "OpenChunkWriter: unknown size, using buffering chunk writer")
		return fs.ChunkWriterInfo{}, &bufferingChunkWriter{
			f:      f,
			o:      o,
			src:    src,
			remote: remote,
			chunks: make(map[int]*chunkFile),
		}, nil
	}

	uploadInfo, err := o.prepareUpload(ctx, src)

	if err != nil {
		return info, nil, fmt.Errorf("failed to prepare upload: %w", err)
	}

	chunkWriter := &objectChunkWriter{
		size:       src.Size(),
		f:          f,
		src:        src,
		o:          o,
		uploadInfo: uploadInfo,
	}
	info = fs.ChunkWriterInfo{
		ChunkSize:         uploadInfo.chunkSize,
		Concurrency:       o.fs.opt.UploadConcurrency,
		LeavePartsOnError: true,
	}
	fs.Debugf(o, "open chunk writer: started upload: %v", uploadInfo.uploadID)
	return info, chunkWriter, err
}

// CreateDir makes a directory with pathID as parent and name leaf
func (f *Fs) CreateDir(ctx context.Context, pathID, leaf string) (newID string, err error) {
	opts := rest.Opts{
		Method: "POST",
		Path:   "/api/files",
	}
	mkdir := api.CreateFileRequest{
		Name:     leaf,
		Type:     "folder",
		ParentId: pathID,
	}
	info := api.FileInfo{}
	err = f.pacer.Call(func() (bool, error) {
		resp, err := f.srv.CallJSON(ctx, &opts, &mkdir, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return "", err
	}
	return info.Id, nil
}

// Mkdir makes the directory (container, bucket)
//
// Shouldn't return an error if it already exists
func (f *Fs) Mkdir(ctx context.Context, dir string) (err error) {
	_, err = f.dirCache.FindDir(ctx, dir, true)
	return err
}

func (f *Fs) purgeCheck(ctx context.Context, dir string, check bool) error {
	root := path.Join(f.root, dir)
	if root == "" {
		return errors.New("can't purge root directory")
	}
	directoryID, err := f.dirCache.FindDir(ctx, dir, false)
	if err != nil {
		return err
	}

	if check {
		info, err := f.readMetaDataForPath(ctx, dir, &api.MetadataRequestOptions{
			Limit: 1,
			Page:  1,
		})
		if err != nil {
			return err
		}
		if len(info.Files) > 0 {
			return fs.ErrorDirectoryNotEmpty
		}
	}

	opts := rest.Opts{
		Method:     "POST",
		Path:       "/api/files/delete",
		NoResponse: true,
	}
	rm := api.RemoveFileRequest{
		Files: []string{directoryID},
	}
	err = f.pacer.Call(func() (bool, error) {
		resp, err := f.srv.CallJSON(ctx, &opts, &rm, nil)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return err
	}
	f.dirCache.FlushDir(dir)
	return nil
}

// Rmdir removes the directory (container, bucket) if empty
//
// Return an error if it doesn't exist or isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) (err error) {
	return f.purgeCheck(ctx, dir, true)
}

// Purge all files in the directory specified
//
// Implement this if you have a way of deleting all the files
// quicker than just running Remove() on the result of List()
//
// Return an error if it doesn't exist
func (f *Fs) Purge(ctx context.Context, dir string) error {
	return f.purgeCheck(ctx, dir, false)
}

// Move src to this remote using server-side move operations.
//
// This is stored with the remote path given.
//
// It returns the destination Object and a possible error.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantMove
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}

	srcLeaf, srcParentID, err := srcObj.fs.dirCache.FindPath(ctx, src.Remote(), false)
	if err != nil {
		return nil, err
	}

	dstLeaf, directoryID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}

	err = f.moveTo(ctx, srcObj.id, srcLeaf, dstLeaf, srcParentID, directoryID)
	if err != nil {
		return nil, err
	}
	f.dirCache.FlushDir(src.Remote())
	newObj := *srcObj
	newObj.remote = remote
	newObj.fs = f
	return &newObj, nil
}

// DirMove moves src, srcRemote to this remote at dstRemote
// using server-side move operations.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantDirMove

// If destination exists then return fs.ErrorDirExists
func (f *Fs) DirMove(ctx context.Context, src fs.Fs, srcRemote, dstRemote string) error {
	srcFs, ok := src.(*Fs)
	if !ok {
		fs.Debugf(srcFs, "Can't move directory - not same remote type")
		return fs.ErrorCantDirMove
	}
	srcID, srcDirectoryID, srcLeaf, dstDirectoryID, dstLeaf, err := f.dirCache.DirMove(ctx, srcFs.dirCache, srcFs.root, srcRemote, f.root, dstRemote)
	if err != nil {
		return err
	}
	err = f.moveTo(ctx, srcID, srcLeaf, dstLeaf, srcDirectoryID, dstDirectoryID)
	if err != nil {
		return fmt.Errorf("dirmove: failed to move: %w", err)
	}
	srcFs.dirCache.FlushDir(srcRemote)
	return nil
}

func (o *Object) Remove(ctx context.Context) error {
	opts := rest.Opts{
		Method:     "POST",
		Path:       "/api/files/delete",
		NoResponse: true,
	}
	delete := api.RemoveFileRequest{
		Files: []string{o.id},
	}
	err := o.fs.pacer.Call(func() (bool, error) {
		resp, err := o.fs.srv.CallJSON(ctx, &opts, &delete, nil)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return err
	}
	return nil
}

// PublicLink adds a "readable by anyone with link" permission on the given file or folder.
func (f *Fs) PublicLink(ctx context.Context, remote string, expire fs.Duration, unlink bool) (link string, err error) {
	id, err := f.dirCache.FindDir(ctx, remote, false)
	if err == nil {
		fs.Debugf(f, "attempting to share directory '%s'", remote)
	} else {
		fs.Debugf(f, "attempting to share single file '%s'", remote)
		o, err := f.NewObject(ctx, remote)
		if err != nil {
			return "", err
		}
		id = o.(fs.IDer).ID()
	}
	if unlink {
		opts := rest.Opts{
			Method:     "DELETE",
			Path:       "/api/files/" + id + "/share",
			NoResponse: true,
		}
		f.pacer.Call(func() (bool, error) {
			resp, err := f.srv.Call(ctx, &opts)
			return shouldRetry(ctx, resp, err)
		})
		return "", nil
	}

	share, err := f.getFileShare(ctx, id)
	if err != nil {
		if !errors.Is(err, fs.ErrorObjectNotFound) {
			return "", err
		}
		opts := rest.Opts{
			Method:     "POST",
			Path:       "/api/files/" + id + "/share",
			NoResponse: true,
		}
		payload := api.FileShare{}
		if expire < fs.DurationOff {
			dur := time.Now().Add(time.Duration(expire)).UTC()
			payload.ExpiresAt = &dur
		}
		err = f.pacer.Call(func() (bool, error) {
			resp, err := f.srv.CallJSON(ctx, &opts, &payload, nil)
			return shouldRetry(ctx, resp, err)
		})
		if err != nil {
			return "", err
		}
		share, err = f.getFileShare(ctx, id)
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%s/share/%s", f.opt.ApiHost, share.ID), nil
}

// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	var resp *http.Response

	fs.FixRangeOption(options, o.size)

	opts := rest.Opts{
		Method:  "GET",
		Path:    fmt.Sprintf("/api/files/%s/%s", o.id, url.QueryEscape(o.name)),
		Options: options,
	}

	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.srv.Call(ctx, &opts)
		return shouldRetry(ctx, resp, err)
	})

	if err != nil {
		return nil, err
	}
	return resp.Body, err
}

// Copy src to this remote using server-side copy operations.
//
// This is stored with the remote path given.
//
// It returns the destination Object and a possible error.
//
// Will only be called if src.Fs().Name() == f.Name()
//
// If it isn't possible then return fs.ErrorCantCopy
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't copy - not same remote type")
		return nil, fs.ErrorCantCopy
	}
	srcLeaf, srcParentID, err := srcObj.fs.dirCache.FindPath(ctx, src.Remote(), false)
	if err != nil {
		return nil, err
	}
	dstLeaf, directoryID, err := f.dirCache.FindPath(ctx, remote, true)
	if err != nil {
		return nil, err
	}

	if srcParentID == directoryID && dstLeaf == srcLeaf {
		fs.Debugf(src, "Can't copy - change file name")
		return nil, fs.ErrorCantCopy
	}

	opts := rest.Opts{
		Method: "POST",
		Path:   "/api/files/" + srcObj.id + "/copy",
	}
	copy := api.CopyFile{
		Newname:     dstLeaf,
		Destination: directoryID,
		ModTime:     srcObj.ModTime(ctx).UTC(),
	}
	var info api.FileInfo
	err = f.pacer.Call(func() (bool, error) {
		resp, err := f.srv.CallJSON(ctx, &opts, &copy, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, err
	}
	return f.newObjectWithInfo(ctx, remote, &info)
}

// About gets quota information
func (f *Fs) About(ctx context.Context) (usage *fs.Usage, err error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/files/categories",
	}
	var stats []api.CategorySize
	err = f.pacer.Call(func() (bool, error) {
		resp, err := f.srv.CallJSON(ctx, &opts, nil, &stats)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read user info: %w", err)
	}

	total := int64(0)
	for category := range stats {
		total += stats[category].Size
	}
	return &fs.Usage{Used: fs.NewUsageValue(total)}, nil
}

// Fs returns the parent Fs
func (o *Object) Fs() fs.Info {
	return o.fs
}

// Return a string version
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path
func (o *Object) Remote() string {
	return o.remote
}

// ModTime returns the modification time of the object
//
// It attempts to read the objects mtime and if that isn't present the
// LastModified returned in the http headers
func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

func (o *Object) MimeType(ctx context.Context) string {
	return o.mimeType
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return o.size
}

func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	if t != telDriveHash {
		return "", hash.ErrUnsupported
	}

	if o.hash != "" {
		return o.hash, nil
	}

	// Fetch from server if not cached
	var file api.FileInfo
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/files/" + o.id,
	}

	err := o.fs.pacer.Call(func() (bool, error) {
		resp, err := o.fs.srv.CallJSON(ctx, &opts, nil, &file)
		return shouldRetry(ctx, resp, err)
	})

	if err != nil {
		return "", fmt.Errorf("failed to get file hash: %w", err)
	}

	if file.Hash != "" {
		o.hash = file.Hash
		return o.hash, nil
	}

	return "", hash.ErrUnsupported
}

// ID returns the ID of the Object if known, or "" if not
func (o *Object) ID() string {
	return o.id
}

// ParentID implements fs.ParentIDer.
func (o *Object) ParentID() string {
	return o.parentId
}

// Storable returns whether this object is storable
func (o *Object) Storable() bool {
	return true
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	updateInfo := &api.UpdateFileInformation{
		ModTime: Ptr(modTime.UTC()),
	}
	err := o.fs.updateFileInformation(ctx, updateInfo, o.id)
	if err != nil {
		return fmt.Errorf("couldn't update mod time: %w", err)
	}
	o.modTime = modTime
	return nil
}

// DirCacheFlush an optional interface to flush internal directory cache
// DirCacheFlush resets the directory cache - used in testing
// as an optional interface
func (f *Fs) DirCacheFlush() {
	f.dirCache.ResetRoot()
}

// Check the interfaces are satisfied
var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.Copier          = (*Fs)(nil)
	_ fs.Mover           = (*Fs)(nil)
	_ fs.DirMover        = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.MimeTyper       = &Object{}
	_ fs.OpenChunkWriter = (*Fs)(nil)
	_ fs.IDer            = (*Object)(nil)
	_ fs.DirCacheFlusher = (*Fs)(nil)
	_ fs.PublicLinker    = (*Fs)(nil)
	_ fs.ParentIDer      = (*Object)(nil)
	_ fs.Abouter         = (*Fs)(nil)
)
