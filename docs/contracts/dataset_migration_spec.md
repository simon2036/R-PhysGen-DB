# 数据集迁移规范

本规范管理 R-PhysGen-DB 的数据集 schema 演进、版本登记和验证流程。它面向 `raw`、`bronze`、`silver`、`gold`、`extensions` 分层 Parquet 产物，以及由这些产物重建的 DuckDB 查询索引。

本项目当前不使用 Alembic、Prisma Migrate、Django migrations 等传统数据库迁移框架。`data/indexes/*.duckdb` 是可重建发布产物，不是主存储；schema 或数据契约变化应优先通过 pipeline 重建 Parquet 与 DuckDB。

## 版本规则

数据集版本使用 SemVer 数据集版本号：

- `vMAJOR.MINOR.PATCH`
- `vMAJOR.MINOR.PATCH-draft`
- `vMAJOR.MINOR.PATCH-rc.N`

版本递增规则：

- `MAJOR`: 破坏性 schema、语义或主键变化；旧消费者需要修改代码或查询。
- `MINOR`: 兼容性新增，例如新增表、可选列、受控枚举值或非破坏性输出。
- `PATCH`: 数据修复、文档修正、非破坏性校验修正或可重复生成产物修正。
- `-draft`: 本地或开发中数据集版本，允许在同一版本内继续修订。
- `-rc.N`: 发布候选版本，用于正式发布前冻结评审。

`data/lake/gold/VERSION` 是当前构建的数据集版本来源，并且必须和 `data/lake/gold/quality_report.json` 中的 `dataset_version` 保持一致。

## 变更类型

每条迁移记录必须声明 `compatibility`：

- `breaking`: 破坏性变更，例如删除/重命名列、改变主键、改变字段语义或单位。
- `additive`: 兼容性新增，例如新增表、可选列、附加报告或新分层产物。
- `patch`: 非破坏性修复，例如纠正文档、修复错误值、修复验证逻辑。
- `data-only`: 仅数据内容变化，不改变 schema、字段语义或 pipeline 接口。
- `pipeline-only`: 仅构建/验证流程变化，不改变数据契约。

破坏性变更必须提高 `MAJOR` 版本，除非变更只发生在 `-draft` 版本内且没有作为稳定产物发布。

## 重建优先原则

默认处理方式是重跑 pipeline：

```powershell
.venv\Scripts\python.exe pipelines\build_v1_dataset.py
.venv\Scripts\python.exe pipelines\validate_v1_dataset.py
```

以下产物均按可重建处理：

- `data/lake/bronze/*.parquet`
- `data/lake/silver/*.parquet`
- `data/lake/gold/*.parquet`
- `data/lake/extensions/**/*.parquet`
- `data/indexes/*.duckdb`

只有当旧数据无法通过当前 pipeline 从权威输入重建，或者需要保留外部一次性人工整理结果时，才允许在 `scripts/migrations/` 下新增一次性迁移脚本。

## 迁移记录

每次改变数据契约、版本、核心输出或构建/验证规则时，都必须在 `docs/migrations/dataset/` 新增迁移记录。记录文件名格式：

```text
YYYY-MM-DD-vMAJOR.MINOR.PATCH[-draft|-rc.N]-short-description.md
```

每条记录必须包含 YAML front matter：

```yaml
migration_id: 2026-04-24-v1.5.0-draft-baseline
target_version: v1.5.0-draft
compatibility: additive
rebuild_required: true
affected_layers:
  - schemas
  - gold
migration_script: none
review_status: applied
```

字段含义：

- `migration_id`: 全局唯一迁移标识，通常与文件名去掉 `.md` 后一致。
- `target_version`: 迁移完成后对应的数据集版本。
- `compatibility`: 变更类型，必须是本规范允许值之一。
- `rebuild_required`: 是否需要重跑 dataset build/validate。
- `affected_layers`: 受影响层，允许值为 `raw`、`bronze`、`silver`、`gold`、`extensions`、`schemas`、`docs`、`pipeline`、`duckdb`、`tests`、`ci`。
- `migration_script`: 一次性迁移脚本路径；没有脚本时必须写 `none`。
- `review_status`: `draft`、`applied`、`released` 或 `superseded`。

当前 `data/lake/gold/VERSION` 必须至少有一条 `review_status: applied` 或 `review_status: released` 的迁移记录。

## 迁移 PR 要求

涉及数据集迁移的 PR 必须检查并按需更新：

- `schemas/*.yaml`
- `docs/contracts/data_contract.md`
- `docs/migrations/dataset/*.md`
- `data/lake/gold/VERSION`
- `src/r_physgen_db/validate.py` 或相关验证模块
- `tests/` 中的 schema、版本、迁移或契约测试

如果变更影响 DuckDB 可查询表，仍然只更新 pipeline 的 DuckDB 重建逻辑；不要对 `data/indexes/*.duckdb` 做手工原地迁移。

## 验证要求

最低验证命令：

```powershell
.venv\Scripts\pytest.exe -q tests\test_dataset_migrations.py
.venv\Scripts\python.exe pipelines\validate_v1_dataset.py
```

发布前完整验证：

```powershell
.venv\Scripts\python.exe pipelines\build_v1_dataset.py
.venv\Scripts\python.exe pipelines\validate_v1_dataset.py
.venv\Scripts\pytest.exe -q
```

在 Baidu Sync 环境中，Parquet 或 DuckDB 文件可能被同步客户端短暂锁定。迁移 PR 应尽量把逻辑/规范变更与大规模生成产物重写分开提交。
