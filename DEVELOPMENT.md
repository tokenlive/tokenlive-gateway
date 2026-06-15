# 开发环境设置

## 快速开始

### tokenlive-admin

```bash
# 1. 复制配置模板
cp configs/dev/server.toml.example configs/dev/server.toml

# 2. 复制环境变量模板
cp .env.example .env

# 3. 编辑 .env 文件，填入实际的密码和密钥
vim .env

# 4. 启动服务（环境变量会自动注入）
go run main.go server -c configs/dev
```

### tokenlive-gateway

```bash
# 1. 复制配置模板
cp config/local.yml.example config/local.yml

# 2. 复制环境变量模板
cp .env.example .env

# 3. 编辑 .env 文件，填入实际的密码和密钥
vim .env

# 4. 启动服务（环境变量会自动注入）
go run cmd/server/main.go -conf config/local.yml
```

## 配置说明

### 环境变量优先级

配置支持三级优先级（从高到低）：

1. **系统环境变量** - 直接设置 `export VAR_NAME=value`
2. **.env 文件** - 项目根目录的 `.env` 文件
3. **配置文件默认值** - 在 `*.example` 文件中定义的默认值

### 环境变量格式

在配置文件中使用：
- `${VAR_NAME}` - 必需的环境变量
- `${VAR_NAME:default_value}` - 带默认值的环境变量

示例：
```toml
RedisAddr = "${REDIS_ADDR:localhost:6379}"
RedisPassword = "${REDIS_PASSWORD}"
```

## 文件说明

| 文件 | 用途 | 提交到 Git |
|------|------|-----------|
| `*.example` | 配置模板（无敏感信息） | ✅ 是 |
| `.env.example` | 环境变量模板 | ✅ 是 |
| `configs/dev/server.toml` | 实际配置文件 | ❌ 否 |
| `config/local.yml` | 实际配置文件 | ❌ 否 |
| `.env` | 本地环境变量 | ❌ 否 |

## 常见场景

### 场景 1: 本地开发，使用远程 Redis

编辑 `.env` 文件：
```bash
REDIS_ADDR=your-redis-server:6379
REDIS_PASSWORD=your-redis-password
```

### 场景 2: 使用 MySQL 数据库

编辑 `.env` 文件：
```bash
# 取消配置文件中的 MySQL DSN 注释，并设置：
MYSQL_PASSWORD=your-mysql-password
MYSQL_HOST=your-mysql-host
```

### 场景 3: Docker 部署

```yaml
# docker-compose.yml
services:
  app:
    environment:
      - REDIS_PASSWORD=your-password
      - DB_DSN=your-database-dsn
    volumes:
      - ./config/local.yml:/app/config/local.yml
```

或者使用环境变量文件：
```bash
docker run --env-file .env your-image
```

## 安全提示

- ✅ `.env` 文件已被 gitignore，不会提交到 Git
- ✅ 实际配置文件已被 gitignore，不会提交到 Git
- ✅ 只有模板文件（`*.example`）会被提交
- ❌ 不要将包含真实密码的文件提交到版本控制
- ❌ 不要在代码中硬编码密码

## 故障排查

### 问题：配置文件找不到

确保已复制模板文件：
```bash
cp configs/dev/server.toml.example configs/dev/server.toml
```

### 问题：环境变量未生效

1. 检查 `.env` 文件是否存在
2. 检查环境变量名是否正确（区分大小写）
3. 检查配置文件中的占位符格式是否正确

### 问题：默认值不正确

查看 `*.example` 文件中的默认值，可能需要更新。
