# 科学计算任务运营服务

这是一个面向科研平台、实验室和计算中心的 Python 后端，使用 FastAPI 与 SQLite 管理参数模板、策略驱动的工作者注册表、计算任务提交、优先级排队、工作者领取、取消、失败重试、租约恢复、用户配额、结果版本和管理员人工干预记录。服务同时保留用户、角色、会话和审计等基础能力，所有运行数据都在单个本地数据库文件中，不需要另行部署数据库、缓存、消息队列或浏览器界面。

## 已有能力

- 参数模板：保存参数类型、必填项、数值范围、默认值、最大运行时间和最大尝试次数。
- 任务提交：根据模板校验参数，使用用户与幂等键避免重复创建，并保存项目、提交人和输入摘要。
- 工作者注册表：保存逻辑身份 worker_id、实例标识 instance_id、软件版本、登记能力集合、最近心跳、并发上限和启停/隔离状态。
- 排队领取：按优先级和进入队列的顺序分配任务；领取前核对登记能力、版本兼容性（TOWNSHIP_WORKER_MIN_VERSION）、剩余并发槽位，未注册、被隔离、被停用或版本不兼容的实例一律拒绝，领取时把任务租约绑定到具体实例。
- 执行回执：心跳、完成、失败都必须同时携带 worker_id 与当前有效的 instance_id；实例重启沿用逻辑身份但旧会话立即失效，旧实例不能继续领取、续租或回执，未完成租约等待恢复流程回收。
- 自动隔离：工作者连续失败达到阈值（TOWNSHIP_WORKER_FAILURE_THRESHOLD，默认 3）后自动隔离，隔离依据（阈值、连续次数、最近失败明细）写入只追加事件表；租约过期恢复计入连续失败，同样可以触发隔离。
- 管理员处置：可手工隔离、解除隔离或调整启停；解除隔离必须填写原因，事件与累计成败计数等历史永不清除，查询接口始终展示完整隔离依据。
- 离线判定：心跳年龄达到 TOWNSHIP_WORKER_OFFLINE_SECONDS（默认 90 秒）判为离线，支持注入固定时钟做确定性验证。
- 失败恢复：租约过期后可由恢复入口将任务重新排队，达到最大尝试次数的任务转为失败，并把过期租约记为工作者一次失败。
- 配额控制：可保存用户、角色或项目的排队数、运行数和每日提交上限；当前提交路径执行用户配额。
- 结果版本：每次成功回执保存不可变结果、指标摘要和内容摘要，任务指向当前结果版本。
- 人工干预：取消、人工重试、优先级调整和批量操作均保留操作者、原因、前后状态和批次标识。
- 登录与角色：基础管理模块提供管理员初始化、用户、角色、会话和细粒度权限。

## 运行环境

- Python 3.11
- SQLite 3，由 Python 标准库提供
- Linux、macOS 或 Windows

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `./data/compute-operations.db`。可以复制 `.env.example` 并通过 `TOWNSHIP_DATABASE_PATH` 指定其他本地路径。

## 数据库初始化与检查

```bash
python -m app.cli init-db
python -m app.cli check-db
```

## 启动 API

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查：

```bash
curl -sS http://127.0.0.1:8432/api/system/health
```

计算任务摘要位于 `/api/compute/summary`，模板、配额、提交、领取、回执和人工操作接口统一使用 `/api/compute` 前缀。

### 工作者注册与调度接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/compute/workers/register` | 工作者报到或重启注册（worker_id + 新 instance_id、版本、能力、并发上限） |
| POST | `/api/compute/workers/{worker_id}/heartbeat` | 工作者心跳，携带当前 instance_id |
| GET | `/api/compute/workers`、`/api/compute/workers/{worker_id}` | 查询工作者列表与详情：活动租约、失败率、在线状态、当前会话和隔离依据 |
| POST | `/api/compute/workers/{worker_id}/quarantine` | 管理员手工隔离（必须填写原因） |
| POST | `/api/compute/workers/{worker_id}/release` | 管理员解除隔离（必须填写原因，历史不清除） |
| POST | `/api/compute/workers/{worker_id}/enabled` | 管理员启用/停用工作者（必须填写原因） |

领取与回执请求体需要同时提供 `worker_id` 和 `instance_id`；任务租约绑定实例，重启后的新实例只能等待旧租约过期回收后重新领取。

## 测试

```bash
python -m pytest
```

测试覆盖参数规则、幂等提交、配额拒绝、优先级领取、能力匹配、租约续期、失败退避、结果版本、取消、人工重试、批量操作和租约恢复；工作者注册表用例覆盖注册报到、并发槽位、能力与版本闸门、自动隔离、解除隔离留痕、重启会话失效、活动租约与失败率查询以及固定时钟离线判定，并保留身份与既有科学计算模块的回归用例。

## 编译检查

```bash
python -m compileall -q app tests
```

## API 冒烟

```bash
python -m app.cli smoke
python -m app.cli compute-demo
```

`smoke` 在进程内检查根路径和健康接口，`compute-demo` 会创建示例参数模板、提交一个计算任务并让匹配能力的工作者领取，用于快速确认核心运营链路。

## 目录结构

```text
app/
  compute/         工作者注册表、计算模板、配额、任务、结果版本和人工干预
  api/             用户、角色、认证、审计和系统管理接口
  core/            时钟、安全、异常和分页能力
  repositories/    通用 SQLite 查询
  seismic/         既有地震计算示例领域
  services/        身份、审计和通用后台任务服务
  cli.py           初始化、检查和冒烟入口
  database.py      SQLite 连接、事务、表结构与基础权限
tests/             核心、计算运营和身份回归测试
tools/             本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL、busy timeout 和同步写入策略。提交、领取、回执和人工干预使用即时事务；任务领取通过条件更新避免同一条排队记录被重复领取。服务保存 UTC 时间字符串，测试可以注入固定时钟验证退避、租约到期和跨日配额。会话令牌只保存摘要，审计与人工干预记录不会写入明文密码或令牌。
