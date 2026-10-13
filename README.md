# feedsystem

Go 实现的 TikTok 式短视频后端：Feed 流 / 点赞评论关注 / 实时通知 / 热度热榜 / 分片上传。

## 技术栈

| 层              | 技术                                                  |
|-----------------|-------------------------------------------------------|
| 语言 / Web 框架 | Go / Gin                                              |
| 数据库          | MySQL 8.0（GORM）                                     |
| 缓存            | Redis 7（go-redis v9）                                |
| 消息队列        | RabbitMQ（AMQP 1.0）                                  |
| 对象存储        | MinIO（S3 兼容，分片上传）                            |
| 鉴权            | JWT + Redis token 自愈、bcrypt                        |
| 前端            | Vue 3 + TypeScript + Vite（Pinia / Vue Router / SSE） |
| 部署            | Docker Compose                                        |

## 核心特性

- **Feed 流**：三级缓存（L1 本地 5s / L2 Redis 1h / L3 MySQL）+ 冷热分离 + 游标分页
- **事件驱动**：点赞/评论/关注/热度/通知事件经 MQ 异步落库 + 降级直写
- **事务性发件箱（Outbox）**：发布视频 → outbox 表 → 轮询投递 MQ，保证最终一致性
- **实时通知**：SSE 推送信息
- **热度热榜**：Redis 分钟桶（ZINCRBY）+ 快照（ZUNIONSTORE）+ as_of/offset 稳定分页
- **限流**：固定窗口计数（Lua 原子 INCR+EXPIRE）+ 通用中间件工厂
- **评论幂等**：event_id 唯一索引防重复
- **缓存防击穿**：singleflight + Redis 分布式锁
- **缓存一致性**：写路径失效（先 DB 后 Del 缓存）
- **视频上传**：分片上传 + 断点续传（Redis 会话）

## 模块功能与技术栈

| 模块          | 功能                                                                                                               | 技术栈                             |
|---------------|--------------------------------------------------------------------------------------------------------------------|------------------------------------|
| **账号**      | 注册、登录、Refresh Token、改密、登出、按 ID/用户名查询、主页统计（视频数/粉丝数/关注数）                          | JWT + Redis token 自愈、bcrypt     |
| **视频**      | 5MB 分片上传、断点续传、封面上传、发布（Outbox→MQ→时间线）、作者作品、详情三级缓存+防击穿、#话题标签、删除级联清理 | MinIO 分片、Outbox、GORM           |
| **点赞/评价** | 点赞/取消/踩状态机、已赞列表、MQ 异步落库+降级直写、热度更新、SSE 通知                                             | RabbitMQ、Redis                    |
| **评论**      | 发布、删除、列表、@提及通知、MQ 异步+降级、评论数维护、幂等（event_id 唯一索引）                                   | RabbitMQ、GORM                     |
| **关注**      | 关注、取关、粉丝列表、关注列表、粉丝/关注计数、is-followed、SSE 通知                                               | GORM、RabbitMQ                     |
| **Feed**      | 推荐流（全局时间线）、话题流（tag）、热榜、冷热分离、游标分页、liked_status+counts                                 | Redis ZSET、三级缓存、singleflight |
| **热度热榜**  | 分钟桶 ZINCRBY、快照 ZUNIONSTORE、as_of/offset 稳定分页                                                            | Redis                              |
| **通知**      | 点赞/评论/关注/@提及通知、SSE 实时推送、通知列表、未读计数、已读标记                                               | SSE + SSEHub、RabbitMQ             |
| **限流**      | 固定窗口计数（Lua 原子 INCR+EXPIRE）、5 个写接口防滥用                                                             | Redis Lua、Gin 中间件              |

## 未实现（规划 / 参考）

以下模块/功能参考项目 `feedsystem_video_go`，当前暂未实现，可作为后续规划：

| 模块                       | 说明                                               |
|----------------------------|----------------------------------------------------|
| **私信**                   | 发送私信、按对端查看最近 50 条会话                 |
| **关注流**                 | 按关注关系聚合的视频流（listByFollowing）          |
| **点赞榜**                 | 按点赞数排序的视频流（listLikesCount）             |
| **账号能力补全**           | 改名、头像上传、个人简介（bio）                    |
| **Worker 死信队列（DLX）** | 消费失败重试上限 + 死信队列（当前为重试 3 次丢弃） |

## 架构总览

```
前端（Vue dev :5173）
  ├─ /api   → 后端 API :8080（Vite 代理）
  └─ /minio → MinIO :9000（Vite 代理，视频/封面/分片上传）

后端 API（Gin :8080）
  Handler → Service → Repository → MySQL / Redis / MinIO
  启动时自动建表（AutoMigrate）+ 自动建 MinIO bucket
  OutboxPoller 轮询 outbox 表 → 发 MQ 事件

Worker（独立进程）
  消费 MQ：rating / comment / popularity 事件落库

事件流（fan-out）
  点赞/评论/关注 → exchange → 多队列（落库 + 通知 + 热度）
```

## 快速启动

### ① 启动依赖服务（docker-compose）

```bash
docker compose up -d mysql redis rabbitmq minio
docker compose ps   # 确认 4 个服务 healthy
```

### ② 配置

配置文件：`configs/config.yaml`

- 默认 host 为 `localhost`（依赖服务用 docker 且端口映射到宿主时可直接用）
- 生产环境请修改密码/secret

### ③ 启动后端

```bash
# 编译 API + Worker
make build && make build-worker

# 启动 API（:8080，自动建表、建 MinIO bucket）
./bin/feedsystem &

# 启动 Worker（消费 MQ，rating/comment/popularity 落库）
./bin/feedsystem-worker &
```

验证：
```bash
curl http://localhost:8080/healthz
# → {"status":"ok"}
```

### ④ 启动前端（feedsystem_frontend）

前端项目为独立仓库：`git@github.com:Singdile/feedsystem_frontend.git`（说明见该仓库 README）

```bash
git clone git@github.com:Singdile/feedsystem_frontend.git
cd feedsystem_frontend
npm install
npm run dev     # dev :5173，代理 /api → :8080、/minio → :9000
```

浏览器访问 `http://localhost:5173`

### ⑤ 常用命令

```bash
make build            # 编译 API
make build-worker     # 编译 Worker
make build-cleanup    # 编译 ZSET 残留清理工具
make test             # 全量测试
make run              # go run API
```

## 项目结构

```
cmd/
  main.go               # API 入口
  worker/               # Worker 入口（消费 MQ 落库）
  cleanup-timeline/     # 一次性工具：清理 ZSET 残留
internal/
  config/               # 配置加载
  data/                 # 基础设施（MySQL/Redis/RabbitMQ/MinIO）
  http/                 # 路由、handler、中间件
  middleware/           # 鉴权（JWT）、限流（固定窗口）
  model/                # 数据模型
  repository/           # 数据访问
  service/              # 业务逻辑
  worker/               # 异步消费者（时间线/通知/热度）
configs/                # config.yaml
docs/                   # 设计文档
```

## 端口一览

| 服务     | 端口                 |
|----------|----------------------|
| API      | 8080                 |
| MySQL    | 3306                 |
| Redis    | 6379                 |
| RabbitMQ | 5672（15672 管理台） |
| MinIO    | 9000（9001 控制台）  |
| 前端 dev | 5173                 |

