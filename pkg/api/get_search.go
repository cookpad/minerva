package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type GetSearchResponse struct {
	ID       searchID        `json:"search_id"`
	MetaData *searchMetaData `json:"metadata"`
}

func (x MinervaHandler) getMetaData(id searchID) (*searchMetaData, Error) {
	repo := x.newSearchRepo()

	item, err := repo.get(id)
	if err != nil {
		return nil, wrapSystemError(err, http.StatusInternalServerError, "Fail to access DynamoDB")
	} else if item == nil {
		return nil, newUserErrorf(http.StatusNotFound, "Search result is not found: %s", id)
	}

	if item.Status == statusRunning {
		status, err := getAthenaQueryStatus(x.Region, item.AthenaQueryID)
		if err != nil {
			return nil, err
		}

		if status.Status != statusRunning {
			item.CompletedAt = status.CompletedAt
			item.Status = toQueryStatus(status.Status)
			item.OutputPath = status.OutputPath
			item.ScannedSize = status.ScannedSize

			if err := repo.put(item); err != nil {
				return nil, wrapSystemError(err, http.StatusInternalServerError, "Fail to update search item")
			}
		}
	}

	return &searchMetaData{
		Status:         item.Status,
		Query:          item.Query,
		ElapsedSeconds: item.getElapsedSeconds(),
		StartTime:      item.StartTime.Unix(),
		EndTime:        item.EndTime.Unix(),
		SubmittedTime:  *item.CreatedAt,

		outputPath: item.OutputPath,
	}, nil
}

func (x *MinervaHandler) GetSearch(c *gin.Context) (*Response, Error) {
	id := searchID(c.Param("search_id"))

	meta, err := x.getMetaData(id)
	if err != nil {
		return nil, err
	}

	return &Response{
		Code: http.StatusOK,
		Message: GetSearchResponse{
			ID:       id,
			MetaData: meta,
		},
	}, nil
}
