# R-PhysGen-DB P0 范围、交付件与退出标准（v3）

## 1. 背景判断

当前 V1/Wave 2 已经具备稳定数据库底座：分层存储、inventory catalog、observation 长表、canonical/strict/review queue 治理层以及 model-ready 子集都已经成形。P0 的问题不是“数据太少”，而是 V1 到 V1.5 的几个结构性入口尚未闭合：

1. `archive/2026-05-05/methods/` 中仍混有研究蓝图、演示原型和正式 ingest bundle；
2. Phase 2 接口仍偏 placeholder，无法可靠承接量子计算、多工况循环和主动学习；
3. `property_observation.temperature/pressure/phase` 仍是弱语义字符串，难以支持条件敏感属性；
4. `pipeline.py` 的 build flow 仍是大单体，后续扩展会把隐式依赖和回归风险继续放大；
5. readiness 仍偏工程验证，缺少“科研任务是否可执行”的判断。

## 2. P0 定义

P0 是一个过渡层，不是 V2。它的交付目标是“把 V1 变成可扩展的 V1.5 起点”，而不是一次性跑通全库 DFT、混合工质或主动学习闭环。

P0 做五件事：

- 语义收口：明确 reference、source-of-truth、ingested bundle、runtime code、generated artifact 的边界；
- 条件语义正式化：引入 `observation_condition_set` 和稳定 `condition_set_id`；
- pipeline stage 化：拆出 orchestrator、stage registry、stage manifest 和可回放 stage；
- Phase 2 接口升级：定义 request/result/artifact/write-back，而非只保留占位类名；
- 科研任务就绪性验证：把 single-component screening、strict ML、cycle seed 等任务转为可验证规则。

## 3. P0 不做

P0 不做以下事项：

- 不把 mixtures 全量转正为 model-ready 主体；
- 不大规模运行 DFT/MD；
- 不全库回填多工况循环；
- 不删除现有 `gold/model_ready.parquet`、`property_matrix.parquet`、`molecule_master.parquet`；
- 不引入重型外部依赖作为 build 必需项；
- 不把旧 `temperature/pressure/phase` 字段立即删除。

## 4. P0 交付件

文档：

- `docs/plans/p0/p0_scope_and_exit_criteria.md`
- `docs/plans/p0/p0_phase2_interfaces_draft.md`
- `docs/plans/p0/p0_pipeline_stage_refactor_plan.md`
- `docs/plans/p0/p0_validation_rules_draft.md`
- `docs/reviews/p0_review_response_matrix.md`

Schema/config 草案：

- `schemas/drafts/canonical_feature_registry.yaml`
- `schemas/drafts/observation_condition_set.yaml`
- `schemas/drafts/property_observation_v2.yaml`
- `schemas/drafts/normalization_rules.yaml`
- `schemas/drafts/stage_run_manifest.yaml`
- `schemas/drafts/research_task_readiness_rules.yaml`
- `schemas/drafts/mixture_composition.yaml`
- `schemas/drafts/molecule_split_definition.yaml`

代码蓝图与脚本：

- `src/blueprints/pipeline_staged_blueprint.py`
- `scripts/backfill_condition_set.py`
- `scripts/pr_b_equivalence_check.py`

## 5. P0 退出标准

### 5.1 文档标准

- `archive/2026-05-05/methods/` 的 reference/prototype/ingested bundle 边界在 README 或 docs 中明确；
- Phase 2 接口定义 request/result/status/artifact/write-back；
- stage plan 能说明每个 stage 的输入、输出、失败处理、manifest 写回和 resume 语义；
- PR-A/B/C 的验收标准明确，尤其 PR-B 的等价性检查必须进入 CI。

### 5.2 Schema 标准

- `condition_set_id` 有确定性生成规则，且哈希签名可复现；
- `condition_role`、`phase`、`quality_level`、`ml_use_status`、`readiness_status` 有受控词表；
- readiness 规则使用 namespaced `canonical_feature_key`，不绑定 legacy `property_name`；
- `normalization_rule_id` 不再是悬空引用，非空时可被验证；
- stage manifest 允许同一 stage 多次 attempt；
- condition-sensitive feature 的 `condition_set_id` 要求被验证或至少被报告。

### 5.3 工程标准

- `pipeline.py` 已拆为 orchestrator + stages skeleton；
- 每个 stage 可单独触发、跳过、resume；
- stage 失败无论来自显式失败还是未捕获异常，都必须写入 manifest；
- required inputs guard 能阻止静默运行；
- PR-B 前后输出满足等价性检查：行数、列集合、null 率、数值列均值/标准差在设定容差内一致。

### 5.4 验证标准

- 现有 schema/integration/inventory/quality gate 仍通过；
- 新增 `research_task_readiness` 输出 `passed/degraded/failed`；
- `quality_report.json` 增加 `dataset_version` 和 `condition_migration_progress`；
- `stage_run_manifest` 记录 `code_version`、`dataset_version`、artifact 摘要；
- `molecule_split_definition` 或等价产物记录 split provenance。

## 6. 条件字段迁移策略

### 6.1 原则

- 新写入 observation：能解析条件时必须生成 `condition_set_id`；
- 存量 observation：按自动解析、低风险默认、人工标注三类迁移；
- 旧字段在 P0 只作为兼容层，不再作为推荐聚合的唯一条件键。

### 6.2 自动回填

可自动回填的典型情况：

- `thermodynamic.normal_boiling_temperature` / `boiling_point_c`：`condition_role=normal_boiling_point`；
- `thermodynamic.critical_*`：`condition_role=critical_point`；
- CoolProp 饱和态字段：依据 `phase` 与 `Q` 解析为 `saturation_liquid` 或 `saturation_vapor`；
- DFT 气相标准任务：`condition_role=gas_phase_298k`；
- cycle 输出：由 `cycle_case_id + operating_point_hash` 生成 `cycle_operating_point`。

### 6.3 需人工标注

- 自由文本中含“various temperatures”“near ambient”等模糊描述；
- 多来源对同一观察值给出冲突条件；
- 文献只给范围而非单一状态点；
- 混合物 composition 未给出 basis 或 fraction 的情况。

### 6.4 迁移指标

`quality_report.json` 应报告：

```json
{
  "condition_migration_progress": {
    "total_condition_sensitive_rows": 1000,
    "with_condition_set_id": 700,
    "auto_backfilled": 650,
    "manual_review_required": 120,
    "unresolved_text": 30
  }
}
```

P0 可接受目标：condition-sensitive rows 自动或显式回填 ≥ 60%。V1.5 退出目标：≥ 95%。

## 7. 数据版本化策略

版本格式：`vMAJOR.MINOR.PATCH-YYYYMMDD`。

升级触发：

- MAJOR：schema 不兼容、主键改变、gold 输出删除字段；
- MINOR：governance bundle 更新、canonical registry 扩充、新来源族接入；
- PATCH：seed 修订、parser bug fix、人工 curation 修订。

记录位置：

- `data/lake/gold/VERSION`
- `data/lake/gold/quality_report.json`
- `data/lake/bronze/stage_run_manifest.parquet`

兼容承诺：

- PATCH 不改变 `model_ready` 列集合；
- MINOR 可新增列，不删除列；
- MAJOR 前应提前一个 MINOR 版本发 deprecation notice。

## 8. 建议 PR 切分

PR-A：文档 + schema/config + scripts skeleton。  
PR-B：orchestrator + stage modules 平移现有逻辑，强制输出等价性检查。  
PR-C：condition_set 接入 + task readiness + condition migration report。

## 9. P0 完成后的下一步

P0 之后优先顺序应为：

1. 多工况 cycle schema 与小规模回填；
2. degradation/TFA proxy 和 synthesis proxy；
3. quantum pilot；
4. mixture 一等实体化。
