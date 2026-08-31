# 变量定义 (放在最顶层，方便以后修改)
APP_NAME    := feedsystem

# 2. 【核心】默认目标 (必须放在第一个目标位置)
all: tidy build

# 3. 基础开发指令 (Build, Run, Test)
build:
	@echo "=> 🚀 正在构建二进制文件 [$(APP_NAME)]..."
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "-s -w" -o ./bin/$(APP_NAME) ./cmd/main.go

run:
	@echo "=> ⚡ 正在启动应用..."
	go run ./cmd/main.go

test:
	@echo "=> 🧪 正在执行全量单元测试..."
	go test -v -race ./...

tidy:
	@echo "=> 📦 正在整理 go.mod 依赖..."
	go mod tidy
	go fmt ./...

# 4. 清理与帮助 (工具类)
redis:
	docker run -d --name redis-blue -p 6379:6379 redis:latest


clean:
	@echo "=> 🧹 正在清理构建缓存..."
	rm -f $(APP_NAME)
	go clean

help:
	@echo "使用说明："
	@echo "  make (all)             - 整理依赖并编译项目 (默认)"
	@echo "  make run               - 直接运行项目"

# 6. 【统一声明】.PHONY (放在最后或目标上方，防止冲突)
.PHONY: all build run test tidy clean help
