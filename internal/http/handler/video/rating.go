package video

import (
	"feedsystem/internal/service/video"

	"github.com/gin-gonic/gin"
)

type RatingHandler struct {
	svc *video.RatingService
}

func NewRatingHandler(svc *video.RatingService) *RatingHandler {
	return &RatingHandler{
		svc: svc,
	}
}

// SetRating POST /videos/:id/rating
func (h *RatingHandler) SetRating(c *gin.Context) {}

// GetRating GET /videos/:id/rating
func (h *RatingHandler) GetRating(c *gin.Context) {}

// ListLikedVideos GET /videos/me/liked-videos
func (h *RatingHandler) ListLikedVideos(c *gin.Context) {}
