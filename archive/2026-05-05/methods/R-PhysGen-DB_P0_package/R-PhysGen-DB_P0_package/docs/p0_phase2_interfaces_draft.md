# Phase 2 接口草案（P0 Draft）

## 1. 目标

现有 `quantum_calculation`、`cycle_simulation`、`active_learning_queue` 仍停留在 placeholder 级别。
P0 的目标不是立刻实现这些服务，而是把它们定义成“以后任何实现者都能无歧义对接”的接口契约。

核心规则保持不变：

- Phase 2 输出必须回写到 `property_observation` 或其扩展表；
- 所有外部计算、外部服务或手工审阅都必须登记进 `source_manifest`；
- 任何增量结果都必须可以追溯到 `mol_id`、运行方法、工况和产物路径。

## 2. Quantum Calculation 接口草案

### 2.1 Request
建议请求对象至少包含：

- `request_id`
- `mol_id`
- `isomeric_smiles`
- `charge`
- `spin_multiplicity`
- `conformer_generation_method`: ETKDG | MMFF | xTB | user_provided
- `conformer_force_field`: MMFF94 | UFF | xTB-GFN2
- `conformer_count`: int
- `conformer_selection_criterion`: lowest_energy | boltzmann_weighted | all
- `method_family`
- `program`
- `model_chemistry`
- `basis_set`
- `theory_level`
- `solvation_model`
- `target_properties`
- `artifact_root`
- `metadata`

**注意**：`conformer_policy` 已弃用，改用上述四个结构化字段。

method_family_vocabulary:
  - DFT
  - HF
  - post-HF        # CCSD(T), MP2 等
  - semi-empirical  # PM7, xTB 等
  - force-field     # MMFF, UFF 等

**theory_level** 建议统一为字符串格式（如 `B3LYP/6-311+G**/SMD`），同时保留 `method_family` 作为分类枚举，方便过滤查询。

### 2.2 Result
建议结果对象至少包含：

- `request_id`
- `run_id`
- `status`
- `failure_reason`
- `program_version`
- `wall_time_s`
- `artifact_manifest`
- `derived_observations`
- `quality_level`
- `notes`

### 2.3 Write-back 规则
量子结果不直接写入 `model_ready`。
应先落到：

- `source_manifest`
- `property_observation`
- （可选）`quantum_job` / `quantum_artifact` 扩展表

建议优先写回以下属性：

- HOMO
- LUMO
- HOMO-LUMO gap
- dipole moment
- polarizability
- total energy
- zero-point energy
- frequency convergence status

这些值必须带：
- `method`
- `software`
- `basis_set`
- `temperature`（若适用）
- `phase`
- `quality_level`
- `uncertainty`
- `notes`

### 2.4 DFT 基准方法统一规范

为保证项目内量子计算结果的可比性，推荐以下默认标准：

- **推荐泛函/基组**：B3LYP/6-311+G(2d,p) 用于几何优化和频率计算
- **基准验证**：CCSD(T)/cc-pVTZ 单点能用于关键分子的高精度校验
- **溶剂化模型**：SMD（液态性质）/ 气相（电子结构参考态）
- **构象策略**：ETKDG 生成 ≥ 50 构象 → MMFF94 预优化 → DFT 优化取最低能量
- **收敛标准**：SCF tight, geometry opt tight, 频率无虚频（TS 除外）

偏离上述标准的计算必须在 notes 中说明理由，并在 quality_level 中标注相应级别。

## 3. Cycle Simulation 接口草案

### 3.1 Request
建议请求对象至少包含：

- `request_id`
- `mol_id`
- `fluid_name`
- `mixture_composition_json`（可选）
- `cycle_case_id`
- `operating_point`
- `cycle_model`
- `eos_source`
- `compressor_efficiency`
- `artifact_root`
- `metadata`

#### 单组分 vs 混合物设计规则：
- **纯物质场景**：`mol_id` 必填，`mixture_composition_json` 为 null
- **混合物场景**：`mixture_composition_json` 必填（JSON 格式 {mol_id: mole_fraction}），`mol_id` 填主组分或 null

#### CycleOperatingPoint 最小结构定义

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| evaporating_temperature_c | float | 是 | 蒸发温度 (°C) |
| condensing_temperature_c | float | 是 | 冷凝温度 (°C) |
| subcooling_k | float | 是 | 过冷度 (K) |
| superheat_k | float | 是 | 过热度 (K) |
| ambient_temperature_c | float | 否 | 环境温度 (°C) |

**注意**：operating_point_hash 由上述字段的内容哈希生成，与 Result 中的 operating_point_hash 对应。

### 3.2 Result
建议结果对象至少包含：

- `request_id`
- `run_id`
- `status`
- `cycle_case_id`
- `operating_point_hash`
- `artifact_manifest`
- `derived_observations`
- `convergence_flag`
- `warning_flags`
- `notes`

### 3.3 Write-back 规则
循环结果不应只产出摘要分数。
建议至少回写：

- `cop`
- `volumetric_cooling`
- `pressure_ratio`
- `discharge_temperature`
- `evaporating_pressure`
- `condensing_pressure`
- `specific_cooling_capacity`
- `compressor_work`
- `cycle_convergence_flag`

这类 observation 应引用：

- `cycle_case_id`
- `condition_set_id`
- `method`
- `source_id`

## 4. Active Learning Queue 接口草案

### 4.1 Entry
建议队列条目至少包含：

- `queue_entry_id`
- `mol_id`
- `acquisition_strategy`
- `priority_score`
- `uncertainty_score`
- `novelty_score`
- `feasibility_score`
- `hard_constraint_status`
- `recommended_next_action`
- `payload`
- `created_at`: timestamp
- `updated_at`: timestamp
- `campaign_id`: string        # 标识同一批次的 AL 迭代
- `model_version`: string      # 打分时使用的代理模型版本
- `expires_at`: timestamp      # 可选，超时自动降优先级

#### hard_constraint_status 枚举定义

hard_constraint_status_vocabulary:
  - passed
  - failed
  - partially_passed
  - not_evaluated

### 4.2 Allowed next actions
- `run_quantum`
- `run_cycle`
- `manual_curation`
- `literature_search`
- `defer`
- `reject`

### 4.3 设计原则
主动学习队列不直接改数据库主结果。
它只负责：
- 记录为什么某个分子被选中；
- 指向下一步应该补什么标签；
- 保证新数据回流后能够解释“为什么是这批分子”。

## 5. 建议新增扩展表

P0 不强制实现，但建议预留：

- `quantum_job`
- `quantum_artifact`
- `cycle_case`
- `cycle_result`
- `active_learning_queue`
- `active_learning_decision_log`

## 6. 与现有仓库的兼容策略

- `interfaces.py` 先只增加 dataclass 和协议，不接真实后端；
- `build_dataset()` 仍可保持 V1 运行；
- 未来任何 Phase 2 服务只要遵守本草案，就能增量接入，不需要重构 data contract。