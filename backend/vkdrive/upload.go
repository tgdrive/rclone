package vkdrive

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/gofrs/uuid"
	"github.com/rclone/rclone/backend/vkdrive/api"
	"github.com/rclone/rclone/lib/rest"

	"github.com/rclone/rclone/fs"
)

type uploadInfo struct {
	existingChunks []api.UploadPart
	uploadID       string
}

type objectChunkWriter struct {
	chunkSize       int64
	size            int64
	f               *Fs
	uploadID        string
	src             fs.ObjectInfo
	partsToCommitMu sync.Mutex
	partsToCommit   []api.UploadPart
	existingParts   map[int]api.UploadPart
	o               *Object
	totalParts      int64
}

// WriteChunk will write chunk number with reader bytes, where chunk number >= 0
func (w *objectChunkWriter) WriteChunk(ctx context.Context, chunkNumber int, reader io.ReadSeeker) (size int64, err error) {
	if chunkNumber < 0 {
		err := fmt.Errorf("invalid chunk number provided: %v", chunkNumber)
		return -1, err
	}

	chunkNumber += 1

	if existing, ok := w.existingParts[chunkNumber]; ok {
		io.CopyN(io.Discard, reader, existing.Size)
		w.addCompletedPart(existing)
		return existing.Size, nil
	}

	var (
		response       api.VkUploadResponse
		vkUploadServer api.VkUploadServer
	)

	values := url.Values{}
	values.Set("access_token", w.f.opt.VkAccessToken)
	values.Set("v", "5.154")
	opts := rest.Opts{
		Method:      "POST",
		RootURL:     "https://api.vk.com/method/docs.getUploadServer",
		ContentType: "application/x-www-form-urlencoded",
		Body:        strings.NewReader(values.Encode()),
	}
	err = w.f.pacer.Call(func() (bool, error) {
		resp, err := w.f.srv.CallJSON(ctx, &opts, nil, &vkUploadServer)
		return shouldRetry(ctx, resp, err)
	})

	if err != nil {
		return 0, err
	}

	u1, _ := uuid.NewV4()

	name := hex.EncodeToString(u1.Bytes()) + ".pdf"

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

		opts := rest.Opts{
			Method:               "POST",
			RootURL:              vkUploadServer.Response.UploadURL,
			Body:                 reader,
			MultipartContentName: "file",
			MultipartFileName:    name,
		}

		resp, err := w.f.srv.CallJSON(ctx, &opts, nil, &response)
		retry, err := shouldRetry(ctx, resp, err)
		if err != nil {
			fs.Debugf(w.o, "Error sending chunk %d (retry=%v): %v: %#v", chunkNumber, retry, err, err)
		}
		return retry, err

	})

	if err != nil {
		fs.Debugf(w.o, "Error sending chunk %d: %v", chunkNumber, err)
	} else {
		var fileResponse api.VkFileUploadResponse
		values := url.Values{}
		values.Set("access_token", w.f.opt.VkAccessToken)
		values.Set("file", response.File)
		values.Set("v", "5.154")
		opts := rest.Opts{
			Method:      "POST",
			RootURL:     "https://api.vk.com/method/docs.save",
			ContentType: "application/x-www-form-urlencoded",
			Body:        strings.NewReader(values.Encode()),
		}
		err = w.f.pacer.Call(func() (bool, error) {
			resp, err := w.f.srv.CallJSON(ctx, &opts, nil, &fileResponse)
			return shouldRetry(ctx, resp, err)
		})

		if err != nil {
			return 0, err
		}

		payload := api.UploadPart{
			Name:   name,
			Size:   int64(fileResponse.Response.Doc.Size),
			Url:    fileResponse.Response.Doc.URL,
			PartNo: chunkNumber,
		}
		if w.totalParts > 1 {
			opts = rest.Opts{
				Method: "POST",
				Path:   "/api/uploads/" + w.uploadID,
			}
			err = w.f.pacer.Call(func() (bool, error) {
				resp, err := w.f.srv.CallJSON(ctx, &opts, &payload, nil)
				return shouldRetry(ctx, resp, err)
			})

			if err != nil {
				return 0, err
			}
		}

		w.addCompletedPart(payload)
		fs.Debugf(w.o, "Done sending chunk %d", chunkNumber)
	}
	return size, err

}

// add a part number and etag to the completed parts
func (w *objectChunkWriter) addCompletedPart(part api.UploadPart) {
	w.partsToCommitMu.Lock()
	defer w.partsToCommitMu.Unlock()
	w.partsToCommit = append(w.partsToCommit, part)
}

func (w *objectChunkWriter) Close(ctx context.Context) error {

	if w.totalParts != int64(len(w.partsToCommit)) {
		return fmt.Errorf("uploaded failed")
	}

	base, leaf := w.f.splitPathFull(w.src.Remote())

	if base != "/" {
		err := w.f.CreateDir(ctx, base, "")
		if err != nil {
			return err
		}
	}
	opts := rest.Opts{
		Method: "POST",
		Path:   "/api/files",
	}

	sort.Slice(w.partsToCommit, func(i, j int) bool {
		return w.partsToCommit[i].PartNo < w.partsToCommit[j].PartNo
	})

	fileParts := []api.FilePart{}

	for _, part := range w.partsToCommit {
		fileParts = append(fileParts, api.FilePart{Size: part.Size, Url: part.Url})
	}

	payload := api.CreateFileRequest{
		Name:     w.f.opt.Enc.FromStandardName(leaf),
		Type:     "file",
		Path:     base,
		MimeType: fs.MimeType(ctx, w.src),
		Size:     w.src.Size(),
		Parts:    fileParts,
	}

	err := w.f.pacer.Call(func() (bool, error) {
		resp, err := w.f.srv.CallJSON(ctx, &opts, &payload, nil)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return err
	}
	
	if w.totalParts > 1 {
		opts = rest.Opts{
			Method: "DELETE",
			Path:   "/api/uploads/" + w.uploadID,
		}
		err = w.f.pacer.Call(func() (bool, error) {
			resp, err := w.f.srv.Call(ctx, &opts)
			return shouldRetry(ctx, resp, err)
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (*objectChunkWriter) Abort(ctx context.Context) error {
	return nil
}

func (o *Object) prepareUpload(ctx context.Context, src fs.ObjectInfo, options []fs.OpenOption) (*uploadInfo, error) {
	base, leaf := o.fs.splitPathFull(src.Remote())

	modTime := src.ModTime(ctx).UTC().Format(timeFormat)

	uploadID := MD5(fmt.Sprintf("%s:%d:%s", path.Join(base, leaf), src.Size(), modTime))

	var uploadParts []api.UploadPart
	opts := rest.Opts{
		Method: "GET",
		Path:   "/api/uploads/" + uploadID,
	}

	err := o.fs.pacer.Call(func() (bool, error) {
		resp, err := o.fs.srv.CallJSON(ctx, &opts, nil, &uploadParts)
		return shouldRetry(ctx, resp, err)
	})

	if err != nil {
		return nil, err
	}

	return &uploadInfo{existingChunks: uploadParts, uploadID: uploadID}, nil
}
