# P0 范围、交付件与退出标准草案

## 1. 背景判断

当前仓库的 V1 / Wave 2 已经把数据库工程底座做得比较完整：有分层存储、有 inventory catalog、有 observation 长表、有 canonical / strict / review queue 治理层，也有 `model_ready` 子集。
但 P0 仍未完成，原因不是“数据不够多”，而是以下四件事仍然是平台级阻塞项：

1. `methods` 中仍混有研究蓝图、演示原型和正式 ingest bundle 三种角色，协作语义不够清晰；
2. `phase2_interfaces` 仍是占位接口，不能承接量子计算 / 多工况循环 / 主动学习的真实对接；
3. `property_observation` 的温度、压力等条件字段仍是弱语义字符串，后续多工况、多来源、多任务建模会越来越难；
4. `pipeline.py` 的主构建流程仍然过于集中，继续叠加功能会提高维护和回归风险。

## 2. P0 的目标

P0 不追求把 V2 一次做完，而是完成“从稳定 V1 到可扩展 V1.5”的最小改造，使后续的 mixtures、多工况循环、degradation / synthesis proxy、quantum pilot 能够增量接入，而不用推翻现有底座。

P0 的五个目标：

### P0-1 语义收口
明确哪些文件是：
- reference / concept
- production truth
- ingested bundle
- runtime code
- generated artifact

### P0-2 条件语义正式化
让 observation 级数据不再依赖自由文本温压条件，而是能被程序可靠过滤、聚合和回写。

### P0-3 pipeline stage 化
把单一大流程拆成若干可独立测试、可独立回放、可单步失败恢复的 stage。

### P0-4 接口契约升级
把 Phase 2 placeholder 升级成真实 request / response / artifact / write-back 规格。

### P0-5 科研任务就绪性验证
在现有工程一致性验证之外，新增“这个数据集能不能支持某类科研任务”的规则。

## 3. P0 不做什么

P0 不直接做以下工作：

- 不把 mixtures 全量转正为主表；
- 不大规模跑 DFT / MD；
- 不做全库多工况循环回填；
- 不在 P0 内引入新的重型外部依赖；
- 不改动现有 `gold` 输出的主消费路径到完全不兼容。

## 4. P0 交付件

### 文档
1. `docs/p0_scope_and_exit_criteria.md`
2. `docs/p0_phase2_interfaces_draft.md`
3. `docs/p0_pipeline_stage_refactor_plan.md`
4. `docs/p0_validation_rules_draft.md`

### Schema 草案
1. `schemas/drafts/observation_condition_set.yaml`
2. `schemas/drafts/property_observation_v2.yaml`
3. `schemas/drafts/stage_run_manifest.yaml`
4. `schemas/drafts/research_task_readiness_rules.yaml`

### 代码蓝图
1. `src/blueprints/pipeline_staged_blueprint.py`

## 5. P0 的退出标准

P0 结束必须同时满足以下条件：

### 5.1 文档标准
- 仓库内能一眼区分 reference material 与 source-of-truth；
- `phase2_interfaces` 已有可执行级接口草案，而不只是占位名词；
- stage 拆分方案能明确回答“每一阶段的输入、输出、失败处理和产物注册”。

### 5.2 schema 标准
- observation 条件至少支持标准化 temperature / pressure / phase；
- observation 和 condition 之间有明确关联主键；
- pipeline stage 运行记录可追溯；
- task readiness 规则可被 `validate.py` 程序化消费；
- `canonical_feature_registry.yaml 已定义并覆盖所有核心属性`；
- `normalization_rules.yaml 占位草案已就位，消除悬空引用`。

### 5.3 工程标准
- `pipeline.py` 已拆成 orchestrator + stages，而不是继续膨胀；
- 每个 stage 都能独立触发和独立测试；
- 新的 stage 不破坏现有 `model_ready` 产物兼容性。

### 5.4 验证标准
- 现有 schema / integration / inventory / quality gate 全通过；
- 新增 `research_task_readiness` 检查可输出；
- 兼容层能明确说明哪些旧字段仍保留，哪些已弃用；
- `条件字段迁移策略已文档化，回填脚本已交付`；
- `dataset_version 在 stage_run_manifest 和 quality_report 中均有记录`。

## 6. 建议实施顺序

第一步，先落文档和 schema 草案，不碰生产代码。
第二步，只做 `pipeline.py` 的 stage 拆分框架，不改业务逻辑。
第三步，把 `property_observation` 的条件语义和 `validate.py` 的 task readiness 接上。
第四步，再决定 mixtures、多工况 cycle、quantum pilot 先接哪一条。

## 8. 条件字段迁移策略

### 8.1 原则
- 新写入的 observation 必须携带 condition_set_id；
- 存量数据按 condition_role 分批回填；
- 迁移路径区分"可自动解析"与"需人工标注"。

### 8.2 自动回填范围
以下存量 observation 可通过正则解析或规则映射自动回填 condition_set_id：
- property_name 为 boiling_point 且 temperature/pressure 字段为空 → condition_role = normal_boiling_point
- property_name 为 critical_* 系列 → condition_role = critical_point
- source_type 为 CoolProp 且包含温度标记 → 根据温度值匹配 saturation_liquid/saturation_vapor
- DFT 来源且注明 gas phase 298K → condition_role = gas_phase_298k

### 8.3 人工标注范围
以下情况需人工审阅后标注：
- temperature/pressure 为自由文本且含复杂描述（如 "at various temperatures"）
- 多个来源对同一工况给出冲突的条件文本
- 来源未说明工况的 observation → 暂标 condition_role = unspecified

### 8.4 回填脚本交付时间
- PR-C 提交时应包含 backfill 脚本 `scripts/backfill_condition_set.py`
- 脚本应输出迁移报告：已自动回填数 / 待人工标注数 / 总 observation 数
- 迁移覆盖率目标：自动回填 ≥ 70%，人工标注 ≤ 30%

### 8.5 双轨过渡期管理
- P0 过渡期内，Stage 05 的 harmonize 逻辑需同时支持 condition_set_id 和旧 temperature/pressure 字段
- 在 quality_report.json 中新增 condition_migration_progress 指标，追踪迁移进度
- V1.5 结束前，所有 condition-sensitive properties 必须完成迁移，届时旧字段降级为 deprecated

## 9. 数据版本化策略

### 9.1 版本号格式
采用语义化版本号 + 日期后缀：`vMAJOR.MINOR.PATCH-YYYYMMDD`
- 例：`v1.5.0-20260423`

### 9.2 版本升级触发规则
- **MAJOR**：schema 不兼容变更（如删列、改主键）
- **MINOR**：governance bundle 更新、新增 canonical_feature_key、新增 source
- **PATCH**：seed catalog 更新、数据修正、bug fix

### 9.3 版本记录位置
- stage_run_manifest 的 dataset_version 字段
- quality_report.json 的顶层 dataset_version 字段
- data/gold/VERSION 文件（纯文本，记录当前版本号）

### 9.4 版本兼容性承诺
- PATCH 版本升级保证 model_ready.parquet 列集合不变
- MINOR 版本升级可能新增列，但不删除已有列
- MAJOR 版本升级前须提前一个 MINOR 周期发布 deprecation 通知

## 10. 对仓库维护者的操作建议

建议用三次 PR 完成：

- PR-1：docs + schemas drafts
- PR-2：pipeline orchestrator / stages skeleton + run manifest
- PR-3：condition-set 接入 + validation enhancement

这样可以把 P0 风险压到最低。