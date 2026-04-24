# R-PhysGen-DB P0 Draft Package（评审后优化版）

> 版本：v2（2026-04-23 评审反馈后优化）  
> 基线：v1 原始草案 + R-PhysGen-DB_P0_review.md 全部 24 项评审意见

本文件包用于把评审结论直接落成可执行草案，范围只覆盖 P0 级事项。

## 优化摘要

本次优化基于评审报告的 24 项问题，按优先级全部回应：

### 阻塞级修复（P0 内必须完成）

| # | 评审问题 | 修复位置 |
|---|----------|----------|
| 1 | `condition_set_id` 生成规则未定义 | `schemas/drafts/observation_condition_set.yaml` — 新增 SHA256 内容哈希生成规则 |
| 2 | `condition_role` 缺少受控词表 | 同上 — 新增 `condition_role_vocabulary`（9 项枚举） |
| 3 | 混合物组成表达不足 | 同上 — 新增 `mixture_composition_json` 字段 |
| 4 | `value` / `value_num` 二义性规则缺失 | `schemas/drafts/property_observation_v2.yaml` — 新增 `value_num_bound_type` 字段和使用规则 |
| 5 | `normalization_rule_id` 悬空引用 | 同上 — 标记为 `placeholder_for_v2`；新增 `schemas/drafts/normalization_rules.yaml` 占位草案 |
| 17 | Orchestrator 无异常处理与 manifest 写回 | `src/blueprints/pipeline_staged_blueprint.py` — 补充 try/except + `_persist_stage_manifest()` |
| 18 | `required_inputs` / `produced_outputs` 未做依赖检查 | 同上 — 补充 `_check_required_inputs()` input guard |
| 22 | Readiness 属性绑定到列名而非 `canonical_feature_key` | `docs/p0_validation_rules_draft.md` — 全部改为引用 canonical_feature_key |

### 文档化修复（P0 内明确记录）

| # | 评审问题 | 修复位置 |
|---|----------|----------|
| 6 | `quality_level` 词表未定义 | `property_observation_v2.yaml` — 新增 `quality_level_vocabulary`（8 项） |
| 7 | 时间字段类型不当 | `stage_run_manifest.yaml` — 改为 `timestamp[us, UTC]` |
| 8 | `run_id` 作用域未定义 | 同上 — 新增 `parent_run_id` 字段 + resume 语义规则 |
| 9 | Readiness 规则存 Parquet 是反模式 | `research_task_readiness_rules.yaml` — 改为 config 层 YAML |
| 10 | 必须/可选属性语义缺失 | 同上 — 拆分为 `must_have` / `should_have` 结构 |
| 19 | Stage 03/04 EPA SNAP 边界模糊 | `docs/p0_pipeline_stage_refactor_plan.md` — 澄清职责边界 |
| 20 | `code_version` 无自动填充 | 蓝图代码 — 新增 `_auto_fill_versions()` + git describe |

### V1.5 阶段改进（已在 P0 文档中预埋）

| # | 评审问题 | 修复位置 |
|---|----------|----------|
| 11 | 构象采样策略字段粒度不足 | `docs/p0_phase2_interfaces_draft.md` — 拆分为 4 个结构化字段 |
| 12 | `method_family` 命名混淆 | 同上 — 新增 `theory_level` + `method_family_vocabulary` |
| 13 | `operating_point` 结构未定义 | 同上 — 新增 `CycleOperatingPoint` 最小结构定义 |
| 14 | 单组分 vs 混合物未区分 | 同上 — 新增 `mixture_composition_json` 设计规则 |
| 15 | ALQ 缺少时间戳和版本字段 | 同上 — 新增 5 个字段（created_at 等） |
| 16 | `hard_constraint_status` 枚举未定义 | 同上 — 新增枚举（4 项） |
| 21 | Stage 07 过重 | 蓝图代码 + pipeline 文档 — 内部子步骤独立失败处理 |
| 23 | 缺少降级策略 | `docs/p0_validation_rules_draft.md` — 新增三级状态 (passed/degraded/failed) |
| 24 | 缺少模型包含集大小验证 | `research_task_readiness_rules.yaml` — 新增 `minimum_molecule_count` |

### 新增补充内容（评审第七节）

| 缺失项 | 处理方式 |
|--------|----------|
| `canonical_feature_key` 权威定义表 | 新增 `schemas/drafts/canonical_feature_registry.yaml` |
| `normalization_rules` 表 schema | 新增 `schemas/drafts/normalization_rules.yaml`（placeholder_for_v2） |
| 条件字段迁移策略 | 新增 `docs/p0_scope_and_exit_criteria.md` 第 8 节 |
| 数据版本化策略 | 新增 `docs/p0_scope_and_exit_criteria.md` 第 9 节 |
| DFT 基准方法统一规范 | 新增 `docs/p0_phase2_interfaces_draft.md` 第 2.4 节 |
| PR-B 强制验收标准 | 新增 `docs/p0_pipeline_stage_refactor_plan.md` PR-B 节 |

---

## 目录说明

```
docs/
  p0_scope_and_exit_criteria.md        # P0 范围、退出标准 + 迁移策略 + 版本化
  p0_phase2_interfaces_draft.md        # Phase 2 接口契约（量子/循环/AL）
  p0_pipeline_stage_refactor_plan.md   # Pipeline stage 化方案
  p0_validation_rules_draft.md         # 科研任务就绪性验证

schemas/drafts/
  observation_condition_set.yaml       # 条件集合表（含生成规则和词表）
  property_observation_v2.yaml         # 属性观测表（含 value_num 规则和词表）
  stage_run_manifest.yaml              # Stage 运行记录（含版本和 resume 语义）
  research_task_readiness_rules.yaml   # Readiness 规则（config 层，must_have/should_have）
  canonical_feature_registry.yaml      # [新增] canonical_feature_key 权威注册表
  normalization_rules.yaml             # [新增] 归一化规则占位草案

src/blueprints/
  pipeline_staged_blueprint.py         # Pipeline 蓝图（含异常处理和 input guard）
```

## 使用建议

1. 先把 `docs/p0_scope_and_exit_criteria.md` 过一遍，确认 P0 边界和新增的迁移/版本化策略；
2. 审 `schemas/drafts/`，重点关注 `canonical_feature_registry.yaml`（所有属性引用的权威源）；
3. 按 `docs/p0_pipeline_stage_refactor_plan.md` 和 `src/blueprints/pipeline_staged_blueprint.py` 开始拆 `src/r_physgen_db/pipeline.py`；
4. 落地时参照评审优化表逐项勾对，确保无遗漏。
