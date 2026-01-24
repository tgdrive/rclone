// Package object defines some useful Objects
package object

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/textproto"
	"path"
	"strings"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

// StaticObjectInfo is an ObjectInfo which can be constructed from scratch
type StaticObjectInfo struct {
	remote   string
	modTime  time.Time
	size     int64
	storable bool
	hashes   map[hash.Type]string
	fs       fs.Info
	meta     fs.Metadata
	mimeType string
}

// NewStaticObjectInfo returns a static ObjectInfo
// If hashes is nil and fs is not nil, the hash map will be replaced with
// empty hashes of the types supported by the fs.
func NewStaticObjectInfo(remote string, modTime time.Time, size int64, storable bool, hashes map[hash.Type]string, f fs.Info) *StaticObjectInfo {
	info := &StaticObjectInfo{
		remote:   remote,
		modTime:  modTime,
		size:     size,
		storable: storable,
		hashes:   hashes,
		fs:       f,
	}
	if f != nil && hashes == nil {
		set := f.Hashes().Array()
		info.hashes = make(map[hash.Type]string)
		for _, ht := range set {
			info.hashes[ht] = ""
		}
	}
	if f == nil {
		info.fs = MemoryFs
	}
	return info
}

// WithMetadata adds meta to the ObjectInfo
func (i *StaticObjectInfo) WithMetadata(meta fs.Metadata) *StaticObjectInfo {
	i.meta = meta
	return i
}

// WithMimeType adds meta to the ObjectInfo
func (i *StaticObjectInfo) WithMimeType(mimeType string) *StaticObjectInfo {
	i.mimeType = mimeType
	return i
}

// Fs returns read only access to the Fs that this object is part of
func (i *StaticObjectInfo) Fs() fs.Info {
	return i.fs
}

// Remote returns the remote path
func (i *StaticObjectInfo) Remote() string {
	return i.remote
}

// String returns a description of the Object
func (i *StaticObjectInfo) String() string {
	return i.remote
}

// ModTime returns the modification date of the file
func (i *StaticObjectInfo) ModTime(ctx context.Context) time.Time {
	return i.modTime
}

// Size returns the size of the file
func (i *StaticObjectInfo) Size() int64 {
	return i.size
}

// Storable says whether this object can be stored
func (i *StaticObjectInfo) Storable() bool {
	return i.storable
}

// Hash returns the requested hash of the contents
func (i *StaticObjectInfo) Hash(ctx context.Context, h hash.Type) (string, error) {
	if len(i.hashes) == 0 {
		return "", hash.ErrUnsupported
	}
	if hash, ok := i.hashes[h]; ok {
		return hash, nil
	}
	return "", hash.ErrUnsupported
}

// Metadata on the object
func (i *StaticObjectInfo) Metadata(ctx context.Context) (fs.Metadata, error) {
	return i.meta, nil
}

// MimeType returns the content type of the Object if
// known, or "" if not
func (i *StaticObjectInfo) MimeType(ctx context.Context) string {
	return i.mimeType
}

// Check interfaces
var (
	_ fs.ObjectInfo = (*StaticObjectInfo)(nil)
	_ fs.Metadataer = (*StaticObjectInfo)(nil)
	_ fs.MimeTyper  = (*StaticObjectInfo)(nil)
)

// MemoryFs is an in memory Fs, it only supports FsInfo and Put
var MemoryFs memoryFs

// memoryFs is an in memory fs
type memoryFs struct{}

// Name of the remote (as passed into NewFs)
func (memoryFs) Name() string { return "memory" }

// Root of the remote (as passed into NewFs)
func (memoryFs) Root() string { return "" }

// String returns a description of the FS
func (memoryFs) String() string { return "memory" }

// Precision of the ModTimes in this Fs
func (memoryFs) Precision() time.Duration { return time.Nanosecond }

// Returns the supported hash types of the filesystem
func (memoryFs) Hashes() hash.Set { return hash.Supported() }

// Features returns the optional features of this Fs
func (memoryFs) Features() *fs.Features { return &fs.Features{} }

// List the objects and directories in dir into entries.  The
// entries can be returned in any order but should be for a
// complete directory.
//
// dir should be "" to list the root, and should not have
// trailing slashes.
//
// This should return ErrDirNotFound if the directory isn't
// found.
func (memoryFs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	return nil, nil
}

// NewObject finds the Object at remote.  If it can't be found
// it returns the error ErrorObjectNotFound.
func (memoryFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return nil, fs.ErrorObjectNotFound
}

// Put in to the remote path with the modTime given of the given size
//
// May create the object even if it returns an error - if so
// will return the object and the error, otherwise will return
// nil and the error
func (memoryFs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	o := NewMemoryObject(src.Remote(), src.ModTime(ctx), nil)
	return o, o.Update(ctx, in, src, options...)
}

// Mkdir makes the directory (container, bucket)
//
// Shouldn't return an error if it already exists
func (memoryFs) Mkdir(ctx context.Context, dir string) error {
	return errors.New("memoryFs: can't make directory")
}

// Rmdir removes the directory (container, bucket) if empty
//
// Return an error if it doesn't exist or isn't empty
func (memoryFs) Rmdir(ctx context.Context, dir string) error {
	return fs.ErrorDirNotFound
}

var _ fs.Fs = MemoryFs

// MemoryObject is an in memory object
type MemoryObject struct {
	remote   string
	modTime  time.Time
	content  []byte
	meta     fs.Metadata
	fs       fs.Fs
	mimeType string
}

// NewMemoryObject returns an in memory Object with the modTime and content passed in
func NewMemoryObject(remote string, modTime time.Time, content []byte) *MemoryObject {
	return &MemoryObject{
		remote:  remote,
		modTime: modTime,
		content: content,
		fs:      MemoryFs,
	}
}

// WithMetadata adds meta to the MemoryObject
func (o *MemoryObject) WithMetadata(meta fs.Metadata) *MemoryObject {
	o.meta = meta
	return o
}

// WithMimeType adds mimeType to the MemoryObject
func (o *MemoryObject) WithMimeType(mimeType string) *MemoryObject {
	o.mimeType = mimeType
	return o
}

// Content returns the underlying buffer
func (o *MemoryObject) Content() []byte {
	return o.content
}

// Fs returns read only access to the Fs that this object is part of
func (o *MemoryObject) Fs() fs.Info {
	return o.fs
}

// SetFs sets the Fs that this memory object thinks it is part of
// It will ignore nil f
func (o *MemoryObject) SetFs(f fs.Fs) *MemoryObject {
	if f != nil {
		o.fs = f
	}
	return o
}

// Remote returns the remote path
func (o *MemoryObject) Remote() string {
	return o.remote
}

// String returns a description of the Object
func (o *MemoryObject) String() string {
	return o.remote
}

// ModTime returns the modification date of the file
func (o *MemoryObject) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

// Size returns the size of the file
func (o *MemoryObject) Size() int64 {
	return int64(len(o.content))
}

// Storable says whether this object can be stored
func (o *MemoryObject) Storable() bool {
	return true
}

// Hash returns the requested hash of the contents
func (o *MemoryObject) Hash(ctx context.Context, h hash.Type) (string, error) {
	hash, err := hash.NewMultiHasherTypes(hash.Set(h))
	if err != nil {
		return "", err
	}
	_, err = hash.Write(o.content)
	if err != nil {
		return "", err
	}
	return hash.Sums()[h], nil
}

// SetModTime sets the metadata on the object to set the modification date
func (o *MemoryObject) SetModTime(ctx context.Context, modTime time.Time) error {
	o.modTime = modTime
	return nil
}

// Open opens the file for read.  Call Close() on the returned io.ReadCloser
func (o *MemoryObject) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	var offset, limit int64 = 0, -1
	for _, option := range options {
		switch x := option.(type) {
		case *fs.RangeOption:
			offset, limit = x.Decode(o.Size())
		case *fs.SeekOption:
			offset = x.Offset
		default:
			if option.Mandatory() {
				fs.Logf(o, "Unsupported mandatory option: %v", option)
			}
		}
	}
	content := o.content
	offset = max(offset, 0)
	if limit < 0 {
		content = content[offset:]
	} else {
		content = content[offset:min(offset+limit, int64(len(content)))]
	}
	return io.NopCloser(bytes.NewBuffer(content)), nil
}

// Update in to the object with the modTime given of the given size
//
// This reuses the internal buffer if at all possible.
func (o *MemoryObject) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (err error) {
	size := src.Size()
	if size == 0 {
		o.content = nil
	} else if size < 0 || int64(cap(o.content)) < size {
		o.content, err = io.ReadAll(in)
	} else {
		o.content = o.content[:size]
		_, err = io.ReadFull(in, o.content)
	}
	o.modTime = src.ModTime(ctx)
	return err
}

// Remove this object
func (o *MemoryObject) Remove(ctx context.Context) error {
	return errors.New("memoryObject.Remove not supported")
}

// Metadata on the object
func (o *MemoryObject) Metadata(ctx context.Context) (fs.Metadata, error) {
	return o.meta, nil
}

// MimeType on the object
func (o *MemoryObject) MimeType(ctx context.Context) string {
	return o.mimeType
}

// Check interfaces
var (
	_ fs.Object     = (*MemoryObject)(nil)
	_ fs.MimeTyper  = (*MemoryObject)(nil)
	_ fs.Metadataer = (*MemoryObject)(nil)
)

var retryErrorCodes = []int{429, 500, 502, 503, 504}

func shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

func (o *HTTPObject) fetch(ctx context.Context) error {

	headOpts := rest.Opts{
		Method:  "HEAD",
		RootURL: o.url,
	}

	resp, err := o.client.Call(ctx, &headOpts)
	if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		defer resp.Body.Close()
		return o.parseMetadataResponse(resp)
	}
	if resp != nil {
		resp.Body.Close()
	}

	getOpts := rest.Opts{
		Method:  "GET",
		RootURL: o.url,
		ExtraHeaders: map[string]string{
			"Range": "bytes=0-0",
		},
	}

	resp, err = o.client.Call(ctx, &getOpts)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("metadata fetch failed: %s", resp.Status)
	}

	return o.parseMetadataResponse(resp)
}

func (o *HTTPObject) parseMetadataResponse(resp *http.Response) error {
	var filename string

	if o.dstFileNameFromHeader {
		if cd := resp.Header.Get("Content-Disposition"); cd != "" {
			if _, params, err := mime.ParseMediaType(cd); err == nil {
				if val, ok := params["filename"]; ok {
					filename = textproto.TrimString(path.Base(strings.ReplaceAll(val, "\\", "/")))
				}
			}
		}
	} else {
		filename = path.Base(resp.Request.URL.Path)
	}
	o.size = rest.ParseSizeFromHeaders(resp.Header)

	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		if t, err := http.ParseTime(lm); err == nil {
			o.modTime = t
		}
	}
	o.remote = filename
	return nil

}

var HTTPFs httpFs

type httpFs struct{}

func (h httpFs) Features() *fs.Features { return &fs.Features{} }
func (h httpFs) Hashes() hash.Set       { return hash.Set(hash.None) }
func (h httpFs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	return nil, nil
}
func (h httpFs) Mkdir(ctx context.Context, dir string) error { return nil }
func (h httpFs) Name() string                                { return "http" }
func (h httpFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	return nil, fs.ErrorObjectNotFound
}
func (h httpFs) Precision() time.Duration { return time.Nanosecond }
func (h httpFs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, nil
}
func (h httpFs) Rmdir(ctx context.Context, dir string) error { return nil }
func (h httpFs) Root() string                                { return "" }
func (h httpFs) String() string                              { return "http" }

var _ fs.Fs = HTTPFs

type HTTPObject struct {
	p                     *fs.Pacer
	client                *rest.Client
	url                   string
	remote                string
	size                  int64
	modTime               time.Time
	dstFileNameFromHeader bool
}

func NewHTTPObject(ctx context.Context, url string, dstFileNameFromHeader bool) (*HTTPObject, error) {
	ci := fs.GetConfig(ctx)
	o := &HTTPObject{url: url, client: rest.NewClient(fshttp.NewClient(ctx)), dstFileNameFromHeader: dstFileNameFromHeader}
	o.p = fs.NewPacer(ctx, pacer.NewDefault())
	o.p.SetRetries(ci.LowLevelRetries)
	err := o.fetch(ctx)
	if err != nil {
		return nil, err
	}
	return o, nil
}

func (o *HTTPObject) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

func (o *HTTPObject) Remote() string                        { return o.remote }
func (o *HTTPObject) ModTime(ctx context.Context) time.Time { return o.modTime }
func (o *HTTPObject) Size() int64                           { return o.size }
func (o *HTTPObject) Fs() fs.Info                           { return HTTPFs }
func (o *HTTPObject) Storable() bool                        { return true }
func (o *HTTPObject) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	var (
		err error
		res *http.Response
	)

	fs.FixRangeOption(options, o.size)
	err = o.p.Call(func() (bool, error) {
		opts := rest.Opts{
			Method:  "GET",
			RootURL: o.url,
			Options: options,
		}
		res, err = o.client.Call(ctx, &opts)
		return shouldRetry(ctx, res, err)
	})

	if err != nil {
		return nil, fmt.Errorf("Open failed: %w", err)
	}
	return res.Body, nil
}
func (o *HTTPObject) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return nil
}
func (o *HTTPObject) Remove(ctx context.Context) error                        { return nil }
func (o *HTTPObject) SetModTime(ctx context.Context, modTime time.Time) error { return nil }
func (o *HTTPObject) Hash(ctx context.Context, r hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

var (
	_ fs.Object = (*HTTPObject)(nil)
)
