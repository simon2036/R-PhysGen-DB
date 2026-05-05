# pipeline.py Stage 化改造方案（v3）

## 1. 改造目标

当前 `src/r_physgen_db/pipeline.py` 的 `build_dataset()` 同时承担 inventory 读取、远程源获取、身份解析、observation 融合、governance bundle 集成、推荐层生成、结构特征计算、model-ready 输出、验证和 DuckDB 发布。V1 可运行，但继续扩展 mixtures、cycle、quantum、AL 会把隐式依赖推到不可控。

P0 目标是拆成：

```text
orchestrator + context + stage_registry + stages/*
```

并保证输出兼容。

## 2. Stage 划分

| Stage | 名称 | 主要输入 | 主要输出 | 失败策略 |
|---|---|---|---|---|
| 00 | init_run | config, VERSION | run context, manifest attempt | 失败即停止 |
| 01 | load_inventory | seed/manual inputs | seed_catalog_snapshot, manual_observations | 失败即停止 |
| 02 | resolve_identity | seed_catalog | molecule_core, molecule_alias | 可局部重跑 |
| 03 | acquire_global_sources | config | EPA GWP/ODS/SNAP caches | 远程失败可用缓存 |
| 04 | acquire_entity_sources | seed_catalog, molecule_core | NIST/CoolProp/entity artifacts | 单实体失败不阻塞全局 |
| 05 | harmonize_observations | manual/global/entity artifacts | property_observation, observation_condition_set | 解析失败转 QC warning |
| 06 | integrate_governance_bundle | molecule_core, alias, governance bundle | canonical overlay/strict/review queue/extensions | bundle 缺失可跳过但需记录 |
| 07 | build_feature_and_recommendation_layers | observation + molecule_core + canonical | recommended, structure_features, property_matrix | 子步骤独立失败 |
| 08 | build_model_outputs | molecule_master, property_matrix | split, model_dataset_index, model_ready | 失败即停止 |
| 09 | validate_and_publish | all outputs | validation_report, quality_report, DuckDB | validation failed 即停止发布 |

PR-B 为保持旧 `build_dataset()` 输出等价，实际执行顺序为 `00, 01, 02, 03, 04, 06, 05, 07, 08, 09`：
governance bundle 会先扩展 `seed_catalog`、`molecule_core` 和 `molecule_alias`，随后 Stage 05 再用扩展后的 alias graph
融合 manual/EPA/SNAP observation。V1.5 若进一步重构，可再把 Stage 05/06 的边界拆细。

## 3. Stage 03 / Stage 05 边界

EPA SNAP 和 EPA GWP 属于“全局源”，但最终 observation 是 entity-level。边界如下：

- Stage 03 只负责抓取、缓存、解析全局表，并注册 source；
- Stage 03 不做 mol_id 映射，不筛具体分子；
- Stage 05 负责把全局源按 alias/molecule_context 映射成 property/regulatory rows。

这避免 Stage 03 依赖尚未完全稳定的 molecule graph，也避免 Stage 05 再去抓远程源。

## 4. Artifact 传递规则

- stage 之间通过 `ArtifactRef` 和稳定文件产物传递；
- 大对象应落 Parquet/JSON，避免在单个大函数中隐式传 DataFrame；
- logical artifact 可用于配置或内存状态，但生产 stage 输出应优先文件化；
- 每个 artifact 记录 name、path、kind、row_count、checksum、notes；
- input guard 在 stage 启动前检查 required inputs，防止静默失败。

## 5. Manifest 与版本

`stage_run_manifest` 的主键为 `run_id + stage_id + attempt_id`。同一个 stage 在 resume 中可出现多个 attempt。

Stage 00 自动填充：

- `code_version`: `git describe --tags --always`；
- `dataset_version`: `VERSION` 文件，缺失时生成 `v1.5.0-YYYYMMDD-draft`；
- `pipeline_args_json`: CLI 参数；
- `selected_stage_ids_json`: stage 选择或 resume 信息。

## 6. Stage 07 特别规则

Stage 07 包含三个独立子任务：

1. `property_recommended`
2. `structure_features`
3. `molecule_master + property_matrix`

P0 可暂时合并为一个 stage，但必须做到：

- 单分子 RDKit 失败不能让整个 `property_recommended` 失败；
- `property_recommended` 和 `structure_features` 失败分开记录；
- 任一子步骤失败要写入 warnings；
- 全部子步骤失败才算 Stage 07 failed；
- V1.5 拆成 Stage 07a / 07b / 07c。

## 7. PR 切分与验收

### PR-A：Skeleton

提交：

- context/orchestrator/stage_registry；
- stage skeleton；
- stage_run_manifest schema；
- README 和 docs 更新。

验收：

- blueprint/skeleton 可运行；
- 每个 stage 失败能写 manifest；
- selected stages / stop_after 参数可用。

### PR-B：业务逻辑平移

提交：

- 从原 `pipeline.py` 平移现有逻辑到 stages；
- 不改变业务规则；
- 保留旧 CLI 入口。

强制验收：运行 `scripts/pr_b_equivalence_check.py`，对同一份输入比较 PR 前后：

- row count；
- column set；
- null rate；
- numeric mean/std；
- selected key distribution；
- optional checksum/bit-for-bit comparison。

建议阈值：

- row count 和 column set 必须完全一致；
- numeric mean/std 默认容差 `1e-12`，若存在浮点不稳定可放宽到 `1e-9` 并记录原因；
- null rate 必须完全一致。

### PR-C：条件语义与 readiness

提交：

- condition_set backfill；
- property_observation_v2 字段接入；
- research_task_readiness validator；
- quality_report 增加 condition_migration_progress 和 dataset_version。

验收：

- condition-sensitive rows 有迁移进度报告；
- readiness 输出 `passed/degraded/failed`；
- 所有 canonical feature 引用可在 registry 中找到。

## 8. PR-B 实际目标目录

PR-B 实施时保留 `src/r_physgen_db/pipeline.py` 作为兼容 facade，因为现有脚本和测试会导入
`r_physgen_db.pipeline.build_dataset` 以及若干私有 helper。为避免 `pipeline.py` 与同名 package
冲突，生产 stage 代码落在：

```text
src/r_physgen_db/
  pipeline.py                 # public facade + legacy helper compatibility
  pipeline_stages/
    __init__.py
    artifacts.py
    context.py
    orchestrator.py
    stages.py
```

原始目标目录如下，留作 V1.5 更细拆分时参考：

```text
src/r_physgen_db/
  pipeline/
    __init__.py
    orchestrator.py
    context.py
    stage_registry.py
    artifacts.py
    stages/
      stage00_init_run.py
      stage01_load_inventory.py
      stage02_resolve_identity.py
      stage03_acquire_global_sources.py
      stage04_acquire_entity_sources.py
      stage05_harmonize_observations.py
      stage06_integrate_governance_bundle.py
      stage07_build_feature_and_recommendation_layers.py
      stage08_build_model_outputs.py
      stage09_validate_and_publish.py
```

## 9. 现有函数映射

| 现有函数/逻辑 | 目标 stage |
|---|---|
| `_paths()` | context/artifacts |
| `_load_manual_observations()` | Stage 01 |
| `_register_manual_sources()` | Stage 01 |
| `_resolve_pubchem_snapshot()` | Stage 02 / Stage 04，视来源类型 |
| `_fetch_global_sources()` | Stage 03 |
| NIST per-seed fetch/parse | Stage 04 |
| CoolProp observations | Stage 04 |
| `_manual_property_rows()` | Stage 05 |
| `_epa_*_property_rows()` | Stage 05 |
| `_epa_snap_rows()` | Stage 05 |
| `integrate_property_governance_bundle()` | Stage 06 |
| `_select_recommended()` | Stage 07 |
| `_build_structure_features()` | Stage 07 |
| `_build_property_matrix()` | Stage 07 |
| `_build_model_dataset_index()` | Stage 08 |
| `_build_model_ready()` | Stage 08 |
| `_build_quality_report()` | Stage 09 |
| `_build_duckdb_index()` | Stage 09 |

## 10. 风险控制

- P0 不重写算法；
- 每次移动逻辑先写等价性检查；
- 对远程源默认保留 cache fallback；
- 所有新增字段先 nullable；
- 新 validator 先 warning，再在 V1.5 升级为 hard gate。
