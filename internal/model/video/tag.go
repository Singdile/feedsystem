package video

import "regexp"

type Tag struct {
	ID   uint   `gorm:"primary_key" json:"id"`
	Name string `gorm:"type:varchar(30);not null;uniqueIndex" json:"name"`
}

type VideoTag struct {
	VideoID uint `gorm:"primaryKey;not null"`
	TagID   uint `gorm:"primaryKey;index;not null"` // ← tag_id 单独建索引
}

// \p{L} 表示任意的unicode 文字， \p{N} 表示任意的数字
var tagRegex = regexp.MustCompile(`#([\p{L}\p{N}_]+)`)

// ExtractTags 从description中提取tag，tag必须是以 # 开头，并用空格分离的词组
func ExtractTags(text string) []string {
	var tags []string
	seen := map[string]bool{} // 去重
	match := tagRegex.FindAllStringSubmatch(text, -1)
	for _, m := range match {
		if !seen[m[1]] {
			seen[m[1]] = true
			tags = append(tags, m[1])
		}
	}

	return tags
}
