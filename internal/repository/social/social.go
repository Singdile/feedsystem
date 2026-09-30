package social

import (
	"context"
	"errors"
	"feedsystem/internal/model/account"

	"gorm.io/gorm"
)

type socialRepo struct {
	db *gorm.DB
}

func NewSocialRepo(db *gorm.DB) *socialRepo {
	return &socialRepo{
		db: db,
	}
}
func (r *socialRepo) Create(ctx context.Context, fanID, vloggerID uint) error {
	if fanID == vloggerID {
		return nil
	}

	return r.db.Transaction(func(tx *gorm.DB) error {
		var cnt int64
		err := tx.Model(account.Social{}).Where("follower_id = ? AND vlogger_id = ?", fanID, vloggerID).Count(&cnt).Error
		if err != nil {
			return err
		}

		if cnt > 0 { // 已经创建了
			return nil
		}

		err = tx.Create(&account.Social{FollowerID: fanID, VloggerID: vloggerID}).Error
		if err != nil {
			if errors.Is(err, gorm.ErrDuplicatedKey) {
				return nil
			}
			return err
		}

		return nil
	})
}
func (r *socialRepo) Delete(ctx context.Context, fanID, vloggerID uint) error {
	return r.db.WithContext(ctx).
		Where("follower_id = ? AND vlogger_id = ?", fanID, vloggerID).
		Delete(&account.Social{}).Error
}

func (r *socialRepo) ListVloggerIDByFollow(ctx context.Context, fanID uint) ([]account.Profile, error) {
	var vloggerIds []uint
	err := r.db.Model(account.Social{}).Where("follower_id = ?", fanID).Select("vlogger_id").Find(&vloggerIds).Error
	if err != nil {
		return nil, err
	}

	var users []account.User
	if err := r.db.Model(account.User{}).Where("id IN (?)", vloggerIds).Find(&users).Error; err != nil {
		return nil, err
	}

	var profiles []account.Profile
	for _, v := range users {
		profiles = append(profiles, v.ToProfile())
	}

	return profiles, nil
}
func (r *socialRepo) ListFansByVloggerID(ctx context.Context, vloggerID uint) ([]account.Profile, error) {
	var fanIds []uint
	err := r.db.Model(account.Social{}).Where("vlogger_id = ?", vloggerID).Select("follower_id").Find(&fanIds).Error
	if err != nil {
		return nil, err
	}

	var users []account.User
	err = r.db.Model(account.User{}).Where("id IN (?)", fanIds).Find(&users).Error
	if err != nil {
		return nil, err
	}

	var profiles []account.Profile
	for _, v := range users {
		profiles = append(profiles, v.ToProfile())
	}

	return profiles, err
}
func (r *socialRepo) IsFollowed(ctx context.Context, fanID, vloggerID uint) (bool, error) {
	var cnt int64
	err := r.db.Model(account.Social{}).Where("follower_id = ? AND vlogger_id = ?", fanID, vloggerID).Count(&cnt).Error
	if err != nil {
		return false, err
	}

	if cnt > 0 { // 已经创建了
		return true, nil
	}
	return false, nil
}

func (r *socialRepo) Counts(ctx context.Context, accountID uint) (uint, uint, error) {
	var followingCounts int64
	var followerCounts int64
	err := r.db.Transaction(func(tx *gorm.DB) error {
		err := tx.Model(account.Social{}).Where("follower_id = ?", accountID).Count(&followingCounts).Error
		if err != nil {
			return err
		}

		err = tx.Model(account.Social{}).Where("vlogger_id = ?", accountID).Count(&followerCounts).Error
		if err != nil {
			return err
		}
		return nil
	})

	return uint(followerCounts), uint(followingCounts), err
}
