# Common Commands

这份文档整理了本项目最常用的命令，以及从 0 到 1 跑起整套系统的步骤。

## 0) 前置要求

- 已安装 Docker Desktop（包含 `docker compose`）
- 本机可用 Python 3（用于本地语法检查时）
- 如需运行 `ts-agents`，需要 Node.js 22+ 和 npm
- 如需 Strong Agent 调用 Gemini，准备好 `GEMINI_API_KEY`

## 1) 项目首次启动（推荐路径）

```bash
# 进入项目目录
cd d:\project\ABX\hw\kafka-cluster

# 可选：设置 API key（PowerShell）
$env:GEMINI_API_KEY="your_api_key_here"

# 一键构建并后台启动
docker compose up -d --build

# 查看服务状态
docker compose ps
```

## 2) 常用运行命令

```bash
# 查看全部服务日志（持续输出）
docker compose logs -f

# 只看关键服务日志
docker compose logs -f fast-agent mid-agent strong-agent sink-consumer flink-job

# 重启某个服务
docker compose restart mid-agent

# 重新构建某个服务后启动
docker compose up -d --build strong-agent

# 停止全部服务（保留容器）
docker compose stop

# 停止并删除容器网络
docker compose down

# 停止并删除容器网络 + 卷（会清空数据库/Kafka数据，谨慎）
docker compose down -v
```

## 3) 数据流相关主题（当前架构）

核心流向：

- `source_raw`
- `source_clean`
- `attention_ticket`
- `annotations`
- `decision_packet`
- `embedding_requests`
- `vector_embedding`

## 4) 关键入口地址

- Kafka UI: [http://localhost:8080](http://localhost:8080)
- Flink UI: [http://localhost:8081](http://localhost:8081)
- FastAPI: [http://localhost:8000](http://localhost:8000)
- Frontend: [http://localhost:5173](http://localhost:5173)
- Prometheus: [http://localhost:9090](http://localhost:9090)
- Grafana: [http://localhost:3000](http://localhost:3000)（默认 admin/admin）
- Jaeger: [http://localhost:16686](http://localhost:16686)

## 5) Replay 测试命令

```bash
# 使用 replay 覆盖配置启动（会注入 golden dataset）
docker compose -f docker-compose.yaml -f docker-compose.replay.yaml up -d --build

# 查看 replay source 日志
docker compose -f docker-compose.yaml -f docker-compose.replay.yaml logs -f replay-source
```

## 6) 开发调试（本地语法检查）

```bash
# Python 语法检查（示例）
python -m py_compile llm-worker\llm_worker.py sink-consumer\kafka_to_postgres_sink.py

# 验证 compose 配置合法性
docker compose config -q
```

## 7) TS Agents（pi-mono 适配层）常用命令

```bash
# 进入 TS agents
cd ts-agents

# 安装依赖
npm install

# 构建
npm run build

# 本地直接运行 Mid Agent
npm run mid

# 本地直接运行 Strong Agent
npm run strong
```

## 8) 故障排查速查

```bash
# 看某个服务最近 200 行日志
docker compose logs --tail=200 mid-agent

# 查看容器资源占用
docker stats

# 查看所有容器（含已退出）
docker ps -a

# 强制重建全部镜像并拉起
docker compose up -d --build --force-recreate
```

## 9) 一次性清除并重建（本地目录持久化场景）

你的 Kafka/Postgres 数据是挂载到本地目录（bind mount），仅执行 `docker compose down -v` 不会清掉这些目录数据。  
请用下面这一组命令做“彻底清空 + 重新创建 + 启动”：

```powershell
# 进入项目目录
cd d:\project\ABX\hw\kafka-cluster

# 1) 停服务并移除容器/网络
docker compose down --remove-orphans

# 2) 删除本地持久化目录（Kafka + Postgres）
Remove-Item -Recurse -Force .\kafka1\data, .\kafka2\data, .\kafka3\data, .\postgres\data

# 3) 重新创建空目录
New-Item -ItemType Directory -Force .\kafka1\data, .\kafka2\data, .\kafka3\data, .\postgres\data | Out-Null

# 4) （可选）清理未使用镜像/缓存
docker system prune -f

# 5) 重新构建并启动
docker compose up -d --build
```

如果你还想连 Grafana 的本地状态也一起重置，再额外删除对应挂载目录后再 `up`。

## 10) 手动检查核心库（最新 100 条，每个字段）

> 当前简化链路下推荐检查 7 个核心对象：`raw_messages`（source_raw）、`source_clean`（source_clean）、`embedding_requests`、`vector_embeddings`、`attention_tickets`、`annotations`、`decision_packets`。

```powershell
# 进入项目目录
cd d:\project\ABX\hw\kafka-cluster

# 四个核心对象（你口径里的 4 个“数据库”）
$tables = @(
  "raw_messages",
  "source_clean",
  "vector_embeddings",
  "attention_tickets",
  "annotations",
  "decision_packets"
)

foreach ($t in $tables) {
  Write-Host ""
  Write-Host "==================== $t ====================" -ForegroundColor Cyan
  if ($t -eq "raw_messages") {
    docker compose exec -T postgres psql -U bluesky -d bluesky `
      -c "\x auto" `
      -c "SELECT * FROM raw_messages WHERE kafka_topic = 'source_raw' ORDER BY created_at DESC NULLS LAST LIMIT 100;"
  } elseif ($t -eq "source_clean") {
    docker compose exec -T postgres psql -U bluesky -d bluesky `
      -c "\x auto" `
      -c "SELECT * FROM source_clean ORDER BY created_at DESC NULLS LAST LIMIT 100;"
  } else {
    docker compose exec -T postgres psql -U bluesky -d bluesky `
      -c "\x auto" `
      -c "SELECT * FROM $t ORDER BY created_at DESC NULLS LAST LIMIT 100;"
  }
}
```

如果你希望把每个表的查询结果分别落到各自 `.txt` 文件（便于手动核对）：

```powershell
cd d:\project\ABX\hw\kafka-cluster

$tables = @(
  "raw_messages",
  "source_clean",
  "vector_embeddings",
  "attention_tickets",
  "annotations",
  "decision_packets"
)

# 导出目录
$outDir = ".\manual-check-output"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

foreach ($t in $tables) {
  $file = Join-Path $outDir "$t-latest100.txt"
  Write-Host "Exporting $t -> $file" -ForegroundColor Green
  if ($t -eq "raw_messages") {
    docker compose exec -T postgres psql -U bluesky -d bluesky `
      -c "\x auto" `
      -c "SELECT * FROM raw_messages WHERE kafka_topic = 'source_raw' ORDER BY created_at DESC NULLS LAST LIMIT 100;" `
      2>&1 | Out-File -FilePath $file -Encoding utf8
  } elseif ($t -eq "source_clean") {
    docker compose exec -T postgres psql -U bluesky -d bluesky `
      -c "\x auto" `
      -c "SELECT * FROM source_clean ORDER BY created_at DESC NULLS LAST LIMIT 100;" `
      2>&1 | Out-File -FilePath $file -Encoding utf8
  } else {
    docker compose exec -T postgres psql -U bluesky -d bluesky `
      -c "\x auto" `
      -c "SELECT * FROM $t ORDER BY created_at DESC NULLS LAST LIMIT 100;" `
      2>&1 | Out-File -FilePath $file -Encoding utf8
  }
}
```

如果你想先看字段清单（列名 + 类型）再看数据：

```powershell
cd d:\project\ABX\hw\kafka-cluster

$tables = @(
  "raw_messages",
  "source_clean",
  "vector_embeddings",
  "attention_tickets",
  "annotations",
  "decision_packets"
)

foreach ($t in $tables) {
  Write-Host ""
  Write-Host "==================== COLUMNS: $t ====================" -ForegroundColor Yellow
  docker compose exec -T postgres psql -U bluesky -d bluesky `
    -c "SELECT ordinal_position, column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '$t' ORDER BY ordinal_position;"
}
```

如果字段清单也要按表分别导出：

```powershell
cd d:\project\ABX\hw\kafka-cluster

$tables = @(
  "raw_messages",
  "source_clean",
  "vector_embeddings",
  "attention_tickets",
  "annotations",
  "decision_packets"
)

$outDir = ".\manual-check-output"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

foreach ($t in $tables) {
  $file = Join-Path $outDir "$t-columns.txt"
  Write-Host "Exporting columns $t -> $file" -ForegroundColor Yellow
  docker compose exec -T postgres psql -U bluesky -d bluesky `
    -c "SELECT ordinal_position, column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '$t' ORDER BY ordinal_position;" `
    2>&1 | Out-File -FilePath $file -Encoding utf8
}
```
