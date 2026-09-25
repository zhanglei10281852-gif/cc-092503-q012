# 科研样品全生命周期管理服务

这是一个面向科研机构样品库、实验室和课题组的模块化后端，集中管理样品接收、分装、借用、归还、消耗、销毁、库存盘点、谱系事件、保管位置、异常记录、登录权限、审计以及可恢复后台任务。项目使用 FastAPI 与 SQLite，所有运行数据保存在单个本地数据库文件中，不依赖另行部署的数据库、缓存或消息队列。

## 已有能力

- 身份与权限：支持引导管理员、登录、会话、用户、角色和细粒度权限。
- 批次与二维码：接收批次保存项目、数量和稳定二维码载荷。
- 样品档案：登记样品、数量、单位、保管位置和生命周期状态。
- 分装谱系：一次事务内扣减母样、创建子样、记录损耗与事件链。
- 借用归还：保存借用数量、到期时间、部分归还和最终归还状态。
- 实验消耗：使用幂等键登记消耗，防止重复请求二次扣减。
- 位置脱敏：普通权限只能看到受限位置的替代码，授权人员可查看精确位置。
- 双人审批：高风险操作要求申请人与审批人分离，并累计不同审批人的决定。
- 异常追踪：异常可以关联样品或接收批次，保存严重度和处理状态。
- 离线交接包归并：合作采样站断网交回的交接包携带站点序号、前序摘要、参与人和样品事件，服务在不覆盖原始包的前提下验证哈希链、补齐可连接片段，把分叉、序号空洞、断链、重复事件和跨批次引用送入待裁决队列；后到的前序包触发确定性重算，人工裁决的分支不会被静默改选；只有形成唯一连续链的样品才能正式接收。
- 审计与任务：关键身份及业务操作留痕，后台任务支持去重、领取与完成。

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

默认数据库位于 `./data/samples.db`，可用 `SAMPLE_DATABASE_PATH` 指定其他路径。

## 初始化与完整性检查

```bash
python -m app.cli init-db
python -m app.cli check-db
```

## 启动 API

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 测试

```bash
python -m pytest
```

## 编译检查

```bash
python -m compileall -q app tests
```

## API 冒烟

```bash
python -m app.cli smoke
```

## 离线交接包归并

交接包为 JSON：`station_code`、`station_seq`（站点单调递增序号，从 0 开始）、`prev_digest`（前一包摘要，首包为空串）、`participants`、`events`，以及对上述五个字段规范序列化（键排序、紧凑 JSON、UTF-8）计算的 SHA-256 `digest`。事件包含 `event_id`、`event_type`（collected/sealed/box_changed/received/note）、`sample_code`、`batch_code`、`occurred_at`、`actor`、`detail`。

```bash
# 上传交接包（乱序、重复均可）：重复上传返回 replayed=true 与原归并结果
POST /api/handover/packages

# 站点总览（包哈希校验结果、各样品链状态、未决冲突、当前可推进动作）
GET  /api/handover/stations
GET  /api/handover/stations/{station_code}

# 单条保管链：每段记录的 adopted 与 adoption_reason、全部冲突（含已裁决）
GET  /api/handover/chains/{chain_id}

# 待裁决队列与人工裁决
GET  /api/handover/disputes?status=open
POST /api/handover/disputes/{id}/decisions

# 仅当链状态为 unique（唯一连续、无未决冲突、含采集起点）时允许正式接收
POST /api/handover/chains/{chain_id}/receive
```

归并规则：

- 原始包只追加、从不覆盖；链记录与未决冲突在每次上传后确定性重算。
- 分叉默认采用先到分支，须经 `adopt_branch` 裁决；裁决后即使出现新候选或续包，也不会静默改选（链标记 `locked`）。
- 序号空洞须 `waive` 豁免；前序摘要指向缺失包的断链须 `anchor` 锚定为新起点或弃用；后到的前序包会自动闭合断链。
- 重复事件（相同 `event_id` 或相同内容摘要）默认保留先到副本，可 `adopt` 指定副本或 `reject`；跨批次引用须 `adopt_branch` 指定归属批次；哈希校验失败的包整包不进链。
- 正式接收幂等，并为样品落 `handover.*` 谱系事件；已接收链被封存，后到包不再改写它。
