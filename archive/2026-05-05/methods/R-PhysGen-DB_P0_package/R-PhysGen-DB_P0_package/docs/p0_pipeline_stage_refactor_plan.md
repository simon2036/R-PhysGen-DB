# pipeline.py Stage 化改造方案（P0 Draft）

## 1. 改造目标

当前 `src/r_physgen_db/pipeline.py` 的 `build_dataset()` 承担了过多职责：

- 读 inventory；
- 注册 manual sources；
- 抓 PubChem / NIST / EPA / CoolProp；
- 解析 identity；
- 融 observation；
- 接 property governance bundle；
- 选 recommended / canonical / strict；
- 算结构特征；
- 生成 model dataset；
- 做质量报告；
- 建 DuckDB 索引。

这种写法在 V1 可工作，但对 V1.5 / V2 不友好。
P0 的目标是把它拆成“编排器 + stage 模块”。

## 2. 目标架构

### 2.1 顶层编排器
顶层只保留：

- 参数解析
- stage 选择
- context 初始化
- stage 调度
- 失败恢复 / 跳过 / 断点续跑
- 总报告汇总

### 2.2 stage 模块
建议拆成以下阶段：

#### Stage 00 `init_run`
职责：
- 初始化 run_id
- 建输出目录
- 写 stage_run_manifest 初始记录
- 读取配置
- 自动填充 code_version（通过 `git describe --tags --always`）
- 自动填充 dataset_version（从 VERSION 文件或配置读取）

#### Stage 01 `load_inventory`
职责：
- 读取 `seed_catalog`
- 读取 manual observation 输入
- 读取 aliases / generated supplements
- 做最早期输入合法性检查

#### Stage 02 `resolve_identity`
职责：
- 基于 PubChem 或本地 bulk lookup 解析结构
- 标准化 SMILES / InChI / InChIKey
- 生成 `molecule_core` 与 `molecule_alias`

#### Stage 03 `acquire_global_sources`
职责：
- 抓取 EPA GWP / ODS / SNAP 等全局源
- 注册 source manifest
- 输出全局解析缓存
- 注意：Stage 03 仅负责全局表的获取和缓存（如 EPA GWP 参考表、ODS 列表、SNAP 全量页面）
- entity 级别的 SNAP/EPA 行提取和 mol_id 映射由 Stage 05 的 harmonize 步骤完成
- Stage 03 不进行分子级别的数据筛选

#### Stage 04 `acquire_entity_sources`
职责：
- 逐 seed 获取 NIST / CoolProp / entity-level PubChem
- 记录 resolution status
- 生成 entity 级 raw artifacts

#### Stage 05 `harmonize_observations`
职责：
- 汇总 manual / NIST / CoolProp / EPA / Excel / generated inputs
- 统一 observation 列
- 生成 `property_observation`
- 运行基础 QC

#### Stage 06 `integrate_governance_bundle`
职责：
- 读取 property governance bundle
- 做 crosswalk / canonical overlay / strict / review queue
- 写 extension mirror / audit

#### Stage 07 `build_feature_and_recommendation_layers`
职责：
- 生成 `property_recommended`
- 计算 `structure_features`
- 生成 `molecule_master` / `property_matrix`

> **注意**：Stage 07 内部包含三个语义独立的子步骤：(1) property_recommended 构建，
> (2) structure_features 计算（依赖 RDKit，计算密集），(3) molecule_master / property_matrix 聚合。
> P0 阶段保持合并，但三个子步骤应具备独立失败处理——单分子 RDKit 解析失败不应阻塞其余子步骤。
> V1.5 计划拆分为 Stage 07a / 07b / 07c。

#### Stage 08 `build_model_outputs`
职责：
- 生成 scaffold split
- 生成 `model_dataset_index`
- 生成 `model_ready`

#### Stage 09 `validate_and_publish`
职责：
- 运行 validation
- 汇总 `quality_report`
- 建 DuckDB 索引
- 更新 `stage_run_manifest`

## 3. 关键改造原则

### 3.1 不在 P0 重写业务逻辑
P0 不是重做数据库，只是拆边界。
原则是：
- 先搬函数，不改规则；
- 先 stage 化，再精修算法；
- 先保证 output 等价，再逐步增强语义。

### 3.2 每个 stage 有明确输入输出
每个 stage 都必须回答四个问题：

1. 读取哪些 artifacts？
2. 产出哪些 artifacts？
3. 失败时能否局部重跑？
4. 成功条件是什么？

### 3.3 stage 之间只通过 artifact / context 传递
不要继续把大量 DataFrame 在一整个大函数内部隐式传来传去。
建议：
- 大对象落 parquet / json；
- 小对象走 typed context；
- 所有路径可登记进 `stage_run_manifest`。

## 4. 建议的代码目录

```text
src/r_physgen_db/
  pipeline/
    __init__.py
    orchestrator.py
    context.py
    stage_registry.py
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

## 5. 当前函数到目标 stage 的映射建议

### 映射一：输入层
- `_paths()` -> `context.py`
- `_load_manual_observations()` -> `stage01_load_inventory.py`
- `_register_manual_sources()` -> `stage01_load_inventory.py`

### 映射二：全局源
- `_fetch_global_sources()` -> `stage03_acquire_global_sources.py`

### 映射三：identity / entity source
- `_resolve_pubchem_snapshot()` -> `stage02_resolve_identity.py`
- NIST per-seed 获取逻辑 -> `stage04_acquire_entity_sources.py`
- CoolProp per-seed 获取逻辑 -> `stage04_acquire_entity_sources.py`

### 映射四：observation / governance
- `_manual_property_rows()` -> `stage05_harmonize_observations.py`
- `_epa_gwp_reference_property_rows()` -> `stage05_harmonize_observations.py`
- `_epa_ods_property_rows()` -> `stage05_harmonize_observations.py`
- `_epa_snap_rows()` -> `stage05_harmonize_observations.py`
- `integrate_property_governance_bundle()` -> `stage06_integrate_governance_bundle.py`

### 映射五：gold / model
- `_select_recommended()` -> `stage07_build_feature_and_recommendation_layers.py`
- `_build_structure_features()` -> `stage07_build_feature_and_recommendation_layers.py`
- `_build_property_matrix()` -> `stage07_build_feature_and_recommendation_layers.py`
- `_build_model_dataset_index()` -> `stage08_build_model_outputs.py`
- `_build_model_ready()` -> `stage08_build_model_outputs.py`

### 映射六：publish
- `_build_quality_report()` -> `stage09_validate_and_publish.py`
- `_build_duckdb_index()` -> `stage09_validate_and_publish.py`

## 6. Stage 运行记录建议

建议新增 `stage_run_manifest` 表，至少记录：

- `run_id`
- `stage_id`
- `stage_name`
- `status`
- `started_at`
- `finished_at`
- `input_artifacts_json`
- `output_artifacts_json`
- `row_count_summary_json`
- `error_message`
- `code_version`
- `parser_version`

这样后面出现失败时，才能判断是 source acquisition 问题、identity 问题还是 governance 问题。

### 6.1 code_version 自动填充机制

在 stage00_init_run 中实现：

```python
import subprocess
result = subprocess.run(["git", "describe", "--tags", "--always"], 
                        capture_output=True, text=True)
ctx.state["code_version"] = result.stdout.strip()
```

### 6.2 dataset_version 管理

- 版本号格式：`vMAJOR.MINOR.PATCH-YYYYMMDD`
- 从项目根目录的 `VERSION` 文件读取
- 每次构建自动写入 stage_run_manifest 和 quality_report.json

## 7. P0 的兼容层设计

### 7.1 兼容 CLI
保留现有入口：
- `pipelines/build_v1_dataset.py`

但内部实现改为：
- 调用 `orchestrator.build_dataset_staged(...)`

### 7.2 兼容现有 gold 输出
P0 不改：
- `data/gold/model_ready.parquet`
- `data/gold/property_matrix.parquet`
- `data/gold/molecule_master.parquet`

### 7.3 兼容现有 validate
短期保持 `validate.py` 可用；
中期再把 validate 拆成：
- schema validation
- stage validation
- research task readiness validation

## 8. 建议的 PR 切分

### PR-A
只引入：
- `context.py`
- `stage_registry.py`
- `stage_run_manifest.yaml`
- 空 stage skeleton

### PR-B
把现有逻辑平移进 stage modules，保持行为不变。

**PR-B 强制验收标准**：对同一份种子数据，PR-B 前后 `model_ready.parquet` 的行数、列均值和 null 率必须完全一致（bit-for-bit 或统计等价）。需提交比对脚本 `scripts/pr_b_equivalence_check.py` 作为 PR-B 的 CI 检查。

### PR-C
加入 condition set、task readiness 和增量 validation。

## 9. 失败恢复策略

每个 stage 建议支持：

- `force`
- `skip_if_exists`
- `resume_from`
- `stop_after`

这样你在远程源不稳定时，不需要重跑整个构建。

## 10. 最终验收标准

- `build_dataset()` 不再直接堆叠所有业务；
- 任一 stage 都能单测；
- Stage 失败可追踪到 artifact 和 error；
- 现有质量报告和 gold 输出保持兼容。