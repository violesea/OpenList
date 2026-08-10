package _115_share

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	driver115 "github.com/SheltonZhu/115driver/pkg/driver"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

type Pan115Share struct {
	model.Storage
	Addition
	client  *driver115.Pan115Client
	limiter *rate.Limiter
}

func (d *Pan115Share) Config() driver.Config {
	return config
}

func (d *Pan115Share) GetAddition() driver.Additional {
	return &d.Addition
}

func (d *Pan115Share) Init(ctx context.Context) error {
	if d.LimitRate > 0 {
		d.limiter = rate.NewLimiter(rate.Limit(d.LimitRate), 1)
	}

	return d.login()
}

func (d *Pan115Share) WaitLimit(ctx context.Context) error {
	if d.limiter != nil {
		return d.limiter.Wait(ctx)
	}
	return nil
}

func (d *Pan115Share) Drop(ctx context.Context) error {
	return nil
}

func (d *Pan115Share) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	if err := d.WaitLimit(ctx); err != nil {
		return nil, err
	}
	var ua string
	// TODO: will use user agent from header
	// if args.Header != nil {
	// 	ua = args.Header.Get("User-Agent")
	// }
	if ua == "" {
		ua = base.UserAgentNT
	}
	files := make([]driver115.ShareFile, 0)
	fileResp, err := d.client.GetShareSnapWithUA(ua, d.ShareCode, d.ReceiveCode, dir.GetID(), driver115.QueryLimit(int(d.PageSize)))
	if err != nil {
		return nil, err
	}
	files = append(files, fileResp.Data.List...)
	total := fileResp.Data.Count
	count := len(fileResp.Data.List)
	for total > count {
		fileResp, err := d.client.GetShareSnap(
			d.ShareCode, d.ReceiveCode, dir.GetID(),
			driver115.QueryLimit(int(d.PageSize)), driver115.QueryOffset(count),
		)
		if err != nil {
			return nil, err
		}
		files = append(files, fileResp.Data.List...)
		count += len(fileResp.Data.List)
	}

	return utils.SliceConvert(files, transFunc)
}

func (d *Pan115Share) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	if err := d.WaitLimit(ctx); err != nil {
		return nil, err
	}
	// 始终使用 115Browser UA 获取下载链接，避免 errno 50029（版本过低）
	ua := fmt.Sprintf("Mozilla/5.0 115Browser/%s", getLatestAppVer())
	downloadInfo, err := d.client.DownloadByShareCodeWithUA(ua, d.ShareCode, d.ReceiveCode, file.GetID())
	if err == nil {
		header := http.Header{}
		header.Set("User-Agent", ua)
		return &model.Link{
			URL:    downloadInfo.URL.URL,
			Header: header,
		}, nil
	}
	// 如果分享下载失败（50029 等），自动转存到网盘后走自有文件下载
	log.Infof("[115_share] share download failed (%v), trying auto-transfer...", err)
	return d.linkViaTransfer(ctx, file, args, ua)
}

// linkViaTransfer 自动转存文件到网盘，然后走自有文件下载链接
func (d *Pan115Share) linkViaTransfer(ctx context.Context, file model.Obj, args model.LinkArgs, ua string) (*model.Link, error) {
	// 1. 转存到"最近接收"目录 (cid=0 让115自动放到接收目录)
	transferCID := "0"
	transferData := fmt.Sprintf("share_code=%s&receive_code=%s&file_id=%s&cid=%s",
		d.ShareCode, d.ReceiveCode, file.GetID(), transferCID)

	transferReq := d.client.NewRequest().
		SetHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8").
		SetHeader("Referer", fmt.Sprintf("https://115.com/s/%s?password=%s", d.ShareCode, d.ReceiveCode)).
		SetBody(transferData)

	transferResp := struct {
		State bool            `json:"state"`
		Errno int             `json:"errno"`
		Error string          `json:"error"`
		Data  json.RawMessage `json:"data"`
	}{}

	resp, err := transferReq.Post("https://webapi.115.com/share/receive")
	if err != nil {
		return nil, fmt.Errorf("auto-transfer request failed: %w", err)
	}
	if err := json.Unmarshal(resp.Body(), &transferResp); err != nil {
		return nil, fmt.Errorf("auto-transfer parse failed: %w", err)
	}

	// errno 4200045 = 文件已接收（正常，可继续）
	if !transferResp.State && transferResp.Errno != 4200045 {
		return nil, fmt.Errorf("auto-transfer failed: errno=%d %s", transferResp.Errno, transferResp.Error)
	}
	log.Infof("[115_share] auto-transfer ok for file %s", file.GetName())

	// 2. 在"最近接收"目录查找转存后的文件，获取 pickcode
	listResp := struct {
		State bool                   `json:"state"`
		Data  []map[string]interface{} `json:"data"`
	}{}

	listReq := d.client.NewRequest().SetResult(&listResp)
	// 列出所有最近接收的文件（cid=0 列根目录全部文件）
	_, err = listReq.Get("https://webapi.115.com/files?cid=0&offset=0&limit=100&o=user_id&asc=0&show_dir=0")
	if err != nil {
		return nil, fmt.Errorf("list recent files failed: %w", err)
	}

	var pickcode string
	fileName := file.GetName()
	// 去掉文件扩展名做前缀匹配（转存后可能带 (1) 后缀）
	namePrefix := fileName
	if idx := strings.LastIndex(fileName, "."); idx > 0 {
		namePrefix = fileName[:idx]
	}
	for _, f := range listResp.Data {
		if name, ok := f["n"].(string); ok {
			// 完全匹配 或 前缀匹配（处理 (1) 重复后缀）
			if name == fileName || strings.HasPrefix(name, namePrefix) {
				if pc, ok := f["pc"].(string); ok && pc != "" {
					pickcode = pc
					log.Infof("[115_share] found transferred file: %s pickcode=%s", name, pickcode)
					break
				}
			}
		}
	}
	if pickcode == "" {
		return nil, fmt.Errorf("transferred file not found, total %d files checked", len(listResp.Data))
	}

	// 3. 用 pickcode 获取自有文件下载链接
	downloadInfo, err := d.client.DownloadWithUA(pickcode, ua)
	if err != nil {
		return nil, fmt.Errorf("own-file download failed: %w", err)
	}

	header := http.Header{}
	header.Set("User-Agent", ua)
	return &model.Link{
		URL:    downloadInfo.Url.Url,
		Header: header,
	}, nil
}

func (d *Pan115Share) MakeDir(ctx context.Context, parentDir model.Obj, dirName string) error {
	return errs.NotSupport
}

func (d *Pan115Share) Move(ctx context.Context, srcObj, dstDir model.Obj) error {
	return errs.NotSupport
}

func (d *Pan115Share) Rename(ctx context.Context, srcObj model.Obj, newName string) error {
	return errs.NotSupport
}

func (d *Pan115Share) Copy(ctx context.Context, srcObj, dstDir model.Obj) error {
	return errs.NotSupport
}

func (d *Pan115Share) Remove(ctx context.Context, obj model.Obj) error {
	return errs.NotSupport
}

func (d *Pan115Share) Put(ctx context.Context, dstDir model.Obj, stream model.FileStreamer, up driver.UpdateProgress) error {
	return errs.NotSupport
}

var _ driver.Driver = (*Pan115Share)(nil)
