# 🚀 快速开始

5 分钟内启动项目。

## tokenlive-admin

```bash
# 步骤 1: 复制配置文件
cp configs/dev/server.toml.example configs/dev/server.toml
cp .env.example .env

# 步骤 2: 编辑 .env 文件（填入你的密码）
vim .env

# 步骤 3: 启动服务
go run main.go server -c configs/dev
```

## tokenlive-gateway

```bash
# 步骤 1: 复制配置文件
cp config/local.yml.example config/local.yml
cp .env.example .env

# 步骤 2: 编辑 .env 文件（填入你的密码）
vim .env

# 步骤 3: 启动服务
go run cmd/server/main.go -conf config/local.yml
```

## 必需的环境变量

在 `.env` 文件中至少需要设置：

### Redis（必需）
```bash
REDIS_ADDR=your-redis-host:6379
REDIS_PASSWORD=your-redis-password
```

### 数据库（可选，默认使用 SQLite）
```bash
DB_DSN=your-database-connection-string
```

### API Keys（tokenlive-gateway 必需）
```bash
OPENAI_API_KEY=your-openai-key
ANTHROPIC_API_KEY=your-anthropic-key
```

## 配置优先级

```
系统环境变量 (最高优先级)
    ↓
.env 文件
    ↓
配置文件默认值 (最低优先级)
```

## 文件说明

| 文件 | 说明 | 提交到 Git |
|------|------|-----------|
| `*.example` | 配置模板 | ✅ 是 |
| `.env.example` | 环境变量模板 | ✅ 是 |
| `.env` | 本地环境变量 | ❌ 否 |
| `server.toml` | 实际配置 | ❌ 否 |
| `local.yml` | 实际配置 | ❌ 否 |

## 验证设置

```bash
# 检查配置文件是否存在
ls -lh configs/dev/server.toml config/local.yml

# 检查 .env 文件是否存在
ls -lh .env

# 验证 gitignore 是否正确
git check-ignore configs/dev/server.toml
git check-ignore config/local.yml
```

## 常见问题

**Q: 配置文件找不到？**
A: 确保已复制模板文件：`cp *.example <filename>`

**Q: 环境变量未生效？**
A: 检查 `.env` 文件是否存在，变量名是否正确

**Q: 如何使用 MySQL？**
A: 取消配置文件中的 MySQL DSN 注释，并在 `.env` 中设置数据库密码

## 更多信息

- 详细配置：[CONFIGURATION.md](docs/CONFIGURATION.md)
- 开发指南：[DEVELOPMENT.md](DEVELOPMENT.md)
- 清理总结：[CLEANUP_SUMMARY.md](CLEANUP_SUMMARY.md)
