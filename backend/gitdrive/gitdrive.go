// Package Gitdrive provides an interface to the Gitdrive storage system.
package gitdrive

import (
	"bufio"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/gofrs/uuid"
	"github.com/rclone/rclone/backend/gitdrive/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

const (
	minSleep         = 400 * time.Millisecond // api is extremely rate limited now
	maxSleep         = 5 * time.Second
	decayConstant    = 2            // bigger for slower decay, exponential
	attackConstant   = 0            // start with max sleep
	timeFormat       = time.RFC3339 // 2014-03-07T22:31:12.173Z
	maxChunkSize     = 2 * fs.Gibi
	defaultChunkSize = 500 * fs.Mebi
	minChunkSize     = 100 * fs.Mebi
)

func init() {
	fs.Register(&fs.RegInfo{
		Name:        "gitdrive",
		Description: "Git Drive",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Help:      "Git Token",
			Name:      "git_token",
			Sensitive: true,
		}, {
			Help:      "Api Host",
			Name:      "api_host",
			Sensitive: true,
		}, {
			Help:      "Session Token Cookie",
			Name:      "session_token",
			Sensitive: true,
		}, {
			Help:      "User",
			Name:      "user",
			Sensitive: true,
		}, {
			Name:     "media_proxy",
			Help:     "Media Proxy",
			Advanced: true,
		}, {
			Name:     "chunk_size",
			Default:  defaultChunkSize,
			Help:     "Upload chunk size",
			Advanced: true,
		}, {
			Name:     "upload_resume",
			Default:  true,
			Help:     "Upload Resume",
			Advanced: true,
		}, {

			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			// maxFileLength = 255
			Default: (encoder.Display |
				encoder.EncodeBackQuote |
				encoder.EncodeDoubleQuote |
				encoder.EncodeLtGt |
				encoder.EncodeLeftSpace |
				encoder.EncodeInvalidUtf8),
		}},
	})
}

// Options defines the configuration for this backend
type Options struct {
	ApiHost      string               `config:"api_host"`
	GitToken     string               `config:"git_token"`
	Enc          encoder.MultiEncoder `config:"encoding"`
	User         string               `config:"user"`
	SessionToken string               `config:"session_token"`
	ChunkSize    fs.SizeSuffix        `config:"chunk_size"`
	UploadResume bool                 `config:"upload_resume"`
	DirectStream bool                 `config:"direct_stream"`
	MediaProxy   string               `config:"media_proxy"`
}

// Fs is the interface a cloud storage system must provide
type Fs struct {
	root     string
	name     string
	opt      Options
	features *fs.Features
	srv      *rest.Client
	pacer    *fs.Pacer
}

// Object represents an Gitdrive object
type Object struct {
	fs       *Fs    // what this object is part of
	remote   string // The remote path
	id       string
	size     int64 // Bytes in the object
	parentId string
	name     string
	modTime  string
	parts    []api.Part
	tag      string
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
	return fmt.Sprintf("Gitdrive root '%s'", f.root)
}

// Precision of the ModTimes in this Fs
func (f *Fs) Precision() time.Duration {
	return fs.ModTimeNotSupported
}

// Hashes returns the supported hash types of the filesystem
func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
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

// dirPath returns an escaped file path (f.root, file)
func (f *Fs) dirPath(file string) string {
	//return path.Join(f.diskRoot, file)
	if file == "" || file == "." {
		return "/" + f.root
	}
	return "/" + path.Join(f.root, file)
}

// returns the full path based on root and the last element
func (f *Fs) splitPathFull(pth string) (string, string) {
	fullPath := strings.Trim(path.Join(f.root, pth), "/")

	i := len(fullPath) - 1
	for i >= 0 && fullPath[i] != '/' {
		i--
	}

	if i < 0 {
		return "/" + fullPath[:i+1], fullPath[i+1:]
	}

	// do not include the / at the split
	return "/" + fullPath[:i], fullPath[i+1:]
}

// splitPath is modified splitPath version that doesn't include the separator
// in the base path
func (f *Fs) splitPath(pth string) (string, string) {
	// chop of any leading or trailing '/'
	pth = strings.Trim(pth, "/")

	i := len(pth) - 1
	for i >= 0 && pth[i] != '/' {
		i--
	}

	if i < 0 {
		return pth[:i+1], pth[i+1:]
	}
	return pth[:i], pth[i+1:]
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

	f := &Fs{
		name:  name,
		root:  root,
		opt:   *opt,
		pacer: fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant), pacer.AttackConstant(attackConstant))),
	}
	if root == "/" || root == "." {
		f.root = ""
	} else {
		f.root = strings.Trim(root, "/")
	}
	f.features = (&fs.Features{
		DuplicateFiles:          false,
		CanHaveEmptyDirectories: true,
		ReadMimeType:            false,
	}).Fill(ctx, f)

	client := fshttp.NewClient(ctx)
	authCookie := http.Cookie{Name: "__Secure-next-auth.session-token", Value: opt.SessionToken}
	f.srv = rest.NewClient(client).SetRoot(opt.ApiHost).SetCookie(&authCookie)

	return f, nil
}

func (f *Fs) decodeError(resp *http.Response, response interface{}) (err error) {
	defer fs.CheckClose(resp.Body, &err)

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	// try to unmarshal into correct structure
	err = json.Unmarshal(body, response)
	if err == nil {
		return nil
	}
	// try to unmarshal into Error
	var apiErr api.Error
	err = json.Unmarshal(body, &apiErr)
	if err != nil {
		return err
	}
	return apiErr
}

func (f *Fs) readMetaDataForPath(ctx context.Context, path string, options *api.MetadataRequestOptions) (*api.ReadMetadataResponse, error) {

	op := "list"

	if f.opt.DirectStream {
		op = "listfull"
	}

	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/files",
		Parameters: url.Values{
			"path":          []string{f.opt.Enc.FromStandardPath(path)},
			"perPage":       []string{strconv.FormatUint(options.PerPage, 10)},
			"sort":          []string{"name"},
			"order":         []string{"asc"},
			"op":            []string{op},
			"nextPageToken": []string{options.NextPageToken},
		},
	}
	var err error
	var info api.ReadMetadataResponse
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.Call(ctx, &opts)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, err
	}

	err = f.decodeError(resp, &info)
	if err != nil {
		return nil, err
	}
	return &info, nil
}

func (f *Fs) getPathInfo(ctx context.Context, path string) (*api.ReadMetadataResponse, error) {

	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/files",
		Parameters: url.Values{
			"path": []string{f.opt.Enc.FromStandardPath(path)},
			"op":   []string{"find"},
		},
	}
	var err error
	var info api.ReadMetadataResponse
	var resp *http.Response
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.Call(ctx, &opts)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, err
	}

	err = f.decodeError(resp, &info)
	if err != nil {
		return nil, err
	}
	return &info, nil
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
	root := f.dirPath(dir)

	var limit uint64 = 500        // max number of objects per request
	var nextPageToken string = "" // for the next page of requests

	for {
		opts := &api.MetadataRequestOptions{
			PerPage:       limit,
			NextPageToken: nextPageToken,
		}

		info, err := f.readMetaDataForPath(ctx, root, opts)
		if err != nil {
			if apiErr, ok := err.(api.Error); ok {
				// might indicate other errors but we can probably assume not found here
				if !apiErr.Status {
					return nil, fs.ErrorDirNotFound
				}
			}
			return nil, err
		}

		for _, item := range info.Files {
			remote := path.Join(dir, f.opt.Enc.ToStandardName(item.Name))
			if item.Type == "folder" {
				modTime, _ := time.Parse(timeFormat, item.ModTime)
				d := fs.NewDir(remote, modTime).SetID(item.Id).SetParentID(item.ParentId)
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

		nextPageToken = info.NextPageToken
		//check if we reached end of list
		if nextPageToken == "" {
			break
		}
	}
	return entries, nil
}

// Return an Object from a path
//
// If it can't be found it returns the error fs.ErrorObjectNotFound.
func (f *Fs) newObjectWithInfo(ctx context.Context, remote string, info *api.FileInfo) (fs.Object, error) {
	o := &Object{
		fs:       f,
		remote:   remote,
		id:       info.Id,
		size:     info.Size,
		parentId: info.ParentId,
		name:     info.Name,
		modTime:  info.ModTime,
		parts:    info.Parts,
		tag:      info.Tag,
	}
	return o, nil
}

// NewObject finds the Object at remote.  If it can't be found it
// returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	// no way to directly access an object by path so we have to list the parent dir
	entries, err := f.List(ctx, path.Dir(remote))
	if err != nil {
		// need to change error type
		// if the parent dir doesn't exist the object doesn't exist either
		if err == fs.ErrorDirNotFound {
			return nil, fs.ErrorObjectNotFound
		}
		return nil, err
	}
	for _, entry := range entries {
		if o, ok := entry.(fs.Object); ok {
			if o.Remote() == remote {
				return o, nil
			}
		}
	}
	return nil, fs.ErrorObjectNotFound
}

func (f *Fs) move(ctx context.Context, dstPath string, fileID string) (err error) {

	meta, err := f.getPathInfo(ctx, dstPath)

	if err != nil && len(meta.Files) == 0 {
		return err
	}

	opts := rest.Opts{
		Method: "POST",
		Path:   "/api/files/movefiles",
	}

	mv := api.MoveFileRequest{
		Files:       []string{fileID},
		Destination: meta.Files[0].Id,
	}

	var resp *http.Response
	var info api.UpdateResponse
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, &mv, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return fmt.Errorf("couldn't move file: %w", err)
	}
	if !info.Status {
		return fmt.Errorf("move: api error: %s", info.Message)
	}
	return err
}

// updateFileInformation set's various file attributes most importantly it's name
func (f *Fs) updateFileInformation(ctx context.Context, update *api.UpdateFileInformation, fileId string) (err error) {
	opts := rest.Opts{
		Method: "PATCH",
		Path:   "/api/files/" + fileId,
	}

	var resp *http.Response
	var info api.UpdateResponse
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, update, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return fmt.Errorf("couldn't update file info: %w", err)
	}
	return err
}

func int64min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func MD5(text string) string {
	algorithm := md5.New()
	algorithm.Write([]byte(text))
	return hex.EncodeToString(algorithm.Sum(nil))
}

func (f *Fs) putUnchecked(ctx context.Context, in0 io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {

	base, leaf := f.splitPath(src.Remote())

	fullBase := f.dirPath(base)

	var uploadParts []api.UploadPart

	modTime := src.ModTime(ctx).UTC().Format(timeFormat)

	uploadId := MD5(fmt.Sprintf("%s:%d:%s", path.Join(fullBase, leaf), src.Size(), modTime))

	if f.opt.UploadResume {

		opts := rest.Opts{
			Method: "GET",
			Path:   "/uploads/" + uploadId,
		}

		err := f.pacer.Call(func() (bool, error) {
			resp, err := f.srv.CallJSON(ctx, &opts, nil, &uploadParts)
			return shouldRetry(ctx, resp, err)
		})

		if err != nil {
			return fmt.Errorf("upload not found: %w", err)
		}
	}

	var uploadedSize int64 = 0

	if len(uploadParts) != 0 {
		for _, part := range uploadParts {
			uploadedSize += part.Size
		}
	}

	var release api.Release

	if uploadedSize != src.Size() {
		var releaseId int64

		if len(uploadParts) != 0 {
			releaseId = uploadParts[0].ReleaseId
		} else {
			opts := rest.Opts{
				Method: "GET",
				Path:   "/releases/upload",
			}

			err := f.pacer.Call(func() (bool, error) {
				resp, err := f.srv.CallJSON(ctx, &opts, nil, &release)
				return shouldRetry(ctx, resp, err)
			})

			if err != nil {
				return fmt.Errorf("release not found: %w", err)
			}

			releaseId = release.ReleaseId

		}

		uploadUrl := fmt.Sprintf("https://uploads.github.com/repos/%s/git-storage/releases/%d/assets", f.opt.User, releaseId)

		headers := make(map[string]string)
		headers["accept"] = "application/vnd.github+json"
		headers["authorization"] = fmt.Sprintf("Bearer %s", f.opt.GitToken)
		headers["X-GitHub-Api-Version"] = "2022-11-28"

		in := bufio.NewReader(in0)

		if uploadedSize > 0 {
			io.CopyN(io.Discard, in, uploadedSize)
		}

		left := src.Size() - uploadedSize

		currentPartNo := len(uploadParts) + 1

		for {

			if _, err := in.Peek(1); err != nil {
				if left > 0 {
					return err
				}
				break
			}
			n := int64(f.opt.ChunkSize)
			if src.Size() != -1 {
				n = int64min(left, n)
				left -= n
			}
			partReader := io.LimitReader(in, n)

			u1, _ := uuid.NewV4()
			name := fmt.Sprintf("%s.zip", hex.EncodeToString([]byte(u1.Bytes())))
			opts := rest.Opts{
				Method:        "POST",
				RootURL:       uploadUrl,
				Body:          partReader,
				ExtraHeaders:  headers,
				ContentLength: &n,
				ContentType:   "application/octet-stream",
				Parameters: url.Values{
					"name": []string{name},
				},
			}

			var info api.UploadInfo
			err := f.pacer.Call(func() (bool, error) {
				resp, err := f.srv.CallJSON(ctx, &opts, nil, &info)
				return shouldRetry(ctx, resp, err)
			})
			if err != nil {
				return err
			}

			uploadPart := api.UploadPart{
				UploadID:  uploadId,
				ReleaseId: releaseId,
				Name:      name,
				PartNo:    currentPartNo,
				Id:        info.Id,
				Size:      info.Size}

			if f.opt.UploadResume {
				opts = rest.Opts{
					Method: "POST",
					Path:   "/uploads",
				}
				err = f.pacer.Call(func() (bool, error) {
					resp, err := f.srv.CallJSON(ctx, &opts, &uploadPart, nil)
					return shouldRetry(ctx, resp, err)
				})
				if err != nil {
					return err
				}
			}
			uploadParts = append(uploadParts, uploadPart)
			currentPartNo++
		}
	}

	if fullBase != "/" {
		err := f.Mkdir(ctx, base)
		if err != nil {
			return err
		}
	}

	opts := rest.Opts{
		Method: "POST",
		Path:   "/api/files",
	}

	payload := api.CreateFileRequest{
		Name:      f.opt.Enc.FromStandardName(leaf),
		Type:      "file",
		Path:      fullBase,
		MimeType:  fs.MimeType(ctx, src),
		Size:      src.Size(),
		Parts:     uploadParts,
		ReleaseId: release.ReleaseId,
	}
	err := f.pacer.Call(func() (bool, error) {
		resp, err := f.srv.CallJSON(ctx, &opts, &payload, nil)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return err
	}

	if f.opt.UploadResume {
		opts = rest.Opts{
			Method: "DELETE",
			Path:   "/uploads/" + uploadId,
		}
		err = f.pacer.Call(func() (bool, error) {
			resp, err := f.srv.Call(ctx, &opts)
			return shouldRetry(ctx, resp, err)
		})
		if err != nil {
			return err
		}
	}

	return nil
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
	if src.Size() < 0 {
		return errors.New("refusing to update with unknown size")
	}

	// upload with new size but old name
	//to change
	err := o.fs.putUnchecked(ctx, in, src, options...)
	if err != nil {
		return err
	}

	// delete duplicate object after successful upload
	err = o.Remove(ctx)
	if err != nil {
		return fmt.Errorf("failed to remove old version: %w", err)
	}

	// Fetch new object after deleting the duplicate
	info, err := o.fs.NewObject(ctx, o.Remote())
	if err != nil {
		return err
	}

	// Replace guts of old object with new one
	*o = *info.(*Object)

	return nil
}

// CreateDir dir creates a directory with the given parent path
// base starts from root and may or may not include //
func (f *Fs) CreateDir(ctx context.Context, base string, leaf string) (err error) {

	var resp *http.Response
	var apiErr api.Error
	opts := rest.Opts{
		Method: "POST",
		Path:   "/api/files/makedir",
	}

	dir := base
	if leaf != "" {
		dir = path.Join(dir, leaf)
	}

	if len(dir) == 0 || dir[0] != '/' {
		dir = "/" + dir
	}

	mkdir := api.CreateDirRequest{
		Path: f.opt.Enc.FromStandardPath(dir),
	}
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, &mkdir, &apiErr)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return err
	}
	return nil
}

// Mkdir makes the directory (container, bucket)
//
// Shouldn't return an error if it already exists
func (f *Fs) Mkdir(ctx context.Context, dir string) (err error) {
	if dir == "" || dir == "." {
		return f.CreateDir(ctx, f.root, "")
	}
	return f.CreateDir(ctx, f.root, dir)
}

// may or may not delete folders with contents?
func (f *Fs) purge(ctx context.Context, folderID string) (err error) {
	var resp *http.Response
	var apiErr api.Error
	opts := rest.Opts{
		Method: "POST",
		Path:   "/api/files/deletefiles",
	}
	rm := api.RemoveFileRequest{
		Files: []string{folderID},
	}
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, &rm, &apiErr)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return err
	}
	if !apiErr.Status {
		return apiErr
	}
	return nil
}

// Rmdir removes the directory (container, bucket) if empty
//
// Return an error if it doesn't exist or isn't empty
func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	info, err := f.getPathInfo(ctx, f.dirPath(dir))
	if err != nil || len(info.Files) == 0 {
		return err
	}
	return f.purge(ctx, info.Files[0].Id)
}

// Move src to this remote using server side move operations.
func (f *Fs) Move(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
	srcObj, ok := src.(*Object)
	if !ok {
		fs.Debugf(src, "Can't move - not same remote type")
		return nil, fs.ErrorCantMove
	}

	srcBase, srcLeaf := srcObj.fs.splitPathFull(src.Remote())
	dstBase, dstLeaf := f.splitPathFull(remote)

	needRename := srcLeaf != dstLeaf
	needMove := srcBase != dstBase

	// do the move if required
	if needMove {
		err := f.CreateDir(ctx, strings.Trim(dstBase, "/"), "")
		if err != nil {
			return nil, fmt.Errorf("move: failed to make destination dirs: %w", err)
		}

		err = f.move(ctx, dstBase, srcObj.id)
		if err != nil {
			return nil, err
		}
	}

	// rename to final name if we need to
	if needRename {
		err := f.updateFileInformation(ctx, &api.UpdateFileInformation{
			Name: f.opt.Enc.FromStandardName(dstLeaf),
		}, srcObj.id)
		if err != nil {
			return nil, fmt.Errorf("move: failed final rename: %w", err)
		}
	}

	// copy the old object and apply the changes
	newObj := *srcObj
	newObj.remote = remote
	newObj.fs = f
	return &newObj, nil
}

func (f *Fs) renameDir(ctx context.Context, folderID string, newName string) (err error) {
	var resp *http.Response
	var apiErr api.Error
	opts := rest.Opts{
		Method: "PATCH",
		Path:   "/api/files/" + folderID,
	}
	rename := api.UpdateFileInformation{
		Name: newName,
		Type: "folder",
	}
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, &rename, &apiErr)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return err
	}
	return nil
}

// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {

	fs.FixRangeOption(options, o.size)

	var resp *http.Response

	var opts rest.Opts

	if o.fs.opt.MediaProxy != "" {
		opts = rest.Opts{
			Method:  "GET",
			RootURL: fmt.Sprintf("%s/stream/assets/%s/%s", o.fs.opt.MediaProxy, o.id, o.name),
			Options: options,
		}
	} else {
		opts = rest.Opts{
			Method:  "GET",
			Path:    fmt.Sprintf("/api/files/%s/%s", o.id, o.name),
			Options: options,
		}
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

	// find out source
	srcPath := srcFs.dirPath(srcRemote)
	srcInfo, err := f.getPathInfo(ctx, srcPath)
	if err != nil {
		return fmt.Errorf("dirmove: source not found: %w", err)
	}
	// check if the destination already exists
	dstPath := f.dirPath(dstRemote)
	info, err := f.getPathInfo(ctx, dstPath)
	if len(info.Files) > 0 {
		return fs.ErrorDirExists
	}

	// make the destination parent path
	dstBase, dstName := f.splitPathFull(dstRemote)
	err = f.CreateDir(ctx, dstBase, "")
	if err != nil {
		return fmt.Errorf("dirmove: failed to create dirs: %w", err)
	}

	// find the destination parent dir
	_, err = f.getPathInfo(ctx, dstBase)

	if err != nil {
		return fmt.Errorf("dirmove: failed to read destination: %w", err)
	}
	srcBase, srcName := srcFs.splitPathFull(srcRemote)

	needRename := srcName != dstName
	needMove := srcBase != dstBase

	// do the move
	if needMove {
		opts := rest.Opts{
			Method: "POST",
			Path:   "/api/files/movefiles",
		}
		move := api.MoveFileRequest{
			Files:       []string{srcInfo.Files[0].Id},
			Destination: dstBase,
		}
		var resp *http.Response
		var apiErr api.Error
		err = f.pacer.Call(func() (bool, error) {
			resp, err = f.srv.CallJSON(ctx, &opts, &move, &apiErr)
			return shouldRetry(ctx, resp, err)
		})
		if err != nil {
			return fmt.Errorf("dirmove: failed to move: %w", err)
		}
		if !apiErr.Status {
			return apiErr
		}
	}

	// rename to final name
	if needRename {
		err = f.renameDir(ctx, srcInfo.Files[0].Id, dstName)
		if err != nil {
			return fmt.Errorf("dirmove: failed final rename: %w", err)
		}
	}
	return nil
}

func (f *Fs) About(ctx context.Context) (usage *fs.Usage, err error) {
	var resp *http.Response
	var info api.ReadMetadataResponse
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/files",
		Parameters: url.Values{
			"parentId": []string{"root"},
			"op":       []string{"find"},
		},
	}
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, &opts, nil, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, err
	}
	usage = &fs.Usage{
		Used: fs.NewUsageValue(info.Files[0].Size),
	}
	return usage, nil
}

// Copy src to this remote using server side move operations.
func (f *Fs) Copy(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {

	return nil, nil
}

// ------------------------------------------------------------

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
	modTime, err := time.Parse(timeFormat, o.modTime)
	if err != nil {
		fs.Debugf(o, "Failed to read mtime from object: %v", err)
		return time.Now()
	}
	return modTime
}

// Size returns the size of an object in bytes
func (o *Object) Size() int64 {
	return o.size
}

// Hash returns the Md5sum of an object returning a lowercase hex string
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// ID returns the ID of the Object if known, or "" if not
func (o *Object) ID() string {
	return o.id
}

// Storable returns whether this object is storable
func (o *Object) Storable() bool {
	return true
}

// SetModTime sets the modification time of the local fs object
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return fs.ErrorCantSetModTime
}

func (o *Object) Remove(ctx context.Context) error {
	opts := rest.Opts{
		Method: "POST",
		Path:   "/api/files/deletefiles",
	}
	delete := api.RemoveFileRequest{
		Files: []string{o.id},
	}
	var info api.UpdateResponse
	err := o.fs.pacer.Call(func() (bool, error) {
		resp, err := o.fs.srv.CallJSON(ctx, &opts, &delete, &info)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return err
	}
	if !info.Status {
		return fmt.Errorf("remove: api error: %s", info.Message)
	}
	return nil
}

// Check the interfaces are satisfied
var (
	_ fs.Fs       = (*Fs)(nil)
	_ fs.Copier   = (*Fs)(nil)
	_ fs.Mover    = (*Fs)(nil)
	_ fs.DirMover = (*Fs)(nil)
	_ fs.Object   = (*Object)(nil)
)
