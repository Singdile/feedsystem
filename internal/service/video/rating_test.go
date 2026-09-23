package video

// TODO(rating-tests): RatingService 层单元测试（暂缓，后续补齐）
//
// 需要 mock 的接口（其余传 nil）：
//   - RateRepo (mockRateRepo: SetRating/GetRating/GetRatings/ListLikedVideos)
//   - RatingMQ (mockRatingMQ: PublishRating)
//   - VideoDB  传 nil（4 个方法均未使用）；ObjectStore 构造器不收（死字段）
//
// 计划用例：
//  1. TestSetUserRating
//     - 参数校验: accountID/videoID=0、status 非法 → 400 AppError，且不触发 PublishRating/SetRating
//     - MQ 成功 → 返回 status，SetRating 不被调（正常路径不落库）
//     - MQ 失败 → 降级 SetRating 被调
//     - MQ 为 nil → 直接降级 SetRating
//     - SetRating 落库失败 → 返回 err
//  2. TestGetUserRating
//     - 参数校验(accountID/videoID=0) → 400
//     - 正常 → 透传 repo.GetRating 结果
//     - repo 报错 → 错误透传
//  3. TestGetUserRatings
//     - accountID=0 或空 videoIDs → 400
//     - 正常 → 透传 repo.GetRatings 返回的 map
//  4. TestListLikedVideos
//     - accountID=0 → 400
//     - 空 cursor → repo 收到 nil 指针
//     - 非法 cursor → repo 收到零值 Cursor 指针
//     - 传参为 limit+1
//     - repo 返回 limit+1 条 → 截断为 limit 且 next_cursor 非空
//     - repo 返回 <=limit 条 → next_cursor 为空
//     - repo 报错 → 错误透传
