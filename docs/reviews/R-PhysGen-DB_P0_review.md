# R-PhysGen-DB P0 草案深度评审报告

> 评审日期：2026-04-23  
> 评审范围：`archive/2026-05-05/methods/R-PhysGen-DB_P0_package` 全部文件

---

## 一、总体评价

P0 草案的整体框架是**成熟且克制的**。它正确识别了当前 V1 仓库的四个平台级阻塞项（语义混淆、接口占位、条件字段弱语义、流程单体化），并给出了以"最小改动换取最大扩展性"为目标的改造路线。范围管控（明确列出"P0 不做什么"）、三次 PR 切分、兼容层设计——这些都体现出良好的工程判断。

但草案仍有若干**逻辑空洞、字段歧义和架构盲点**，需要在落地前补齐，否则会在 P0 → V1.5 的过渡阶段埋下技术债。以下按模块逐一展开。

---

## 二、Schema 草案评审

### 2.1 `observation_condition_set.yaml` —— 核心改动，问题最多

**问题 1：`condition_set_id` 生成策略未定义**

这是整个条件语义正式化的主键，但草案没有说明它如何产生：是 UUID、还是 (temperature_value, pressure_value, phase, condition_role) 的哈希？

- 若用 UUID，不同 stage 可能为同一物理条件生成两条记录，后续聚合时会出现爆炸式笛卡尔积；
- 若用内容哈希，哈希函数和字段组合必须作为 schema 的一部分固定下来，否则条件去重无法重现。

**建议**：在 design_rules 中明确写出 `condition_set_id` 的生成规则，例如：

```
condition_set_id = sha256(condition_role + "|" + 
                          str(temperature_value) + temperature_unit + "|" + 
                          str(pressure_value) + pressure_unit + "|" + 
                          phase).hex[:16]
```

并规定"若 temperature 或 pressure 未知，该维度以空字符串参与哈希"。

---

**问题 2：`condition_role` 缺少受控词表**

schema 的 design_rules 给了示例（`normal_boiling_point`, `saturation_point`, `critical_point`），但没有给出完整枚举。对于制冷剂数据库，工况语义对后续循环仿真和 ML 至关重要。

**建议**：在 yaml 中增加 `condition_role_vocabulary` 字段，至少枚举：

```yaml
condition_role_vocabulary:
  - normal_boiling_point       # 1 atm 沸点
  - saturation_liquid          # 饱和液态
  - saturation_vapor           # 饱和气态
  - critical_point             # 临界点
  - supercritical              # 超临界
  - gas_phase_298k             # 298 K 气相（DFT 参考态）
  - cycle_operating_point      # 循环工况点
  - standard_reference_state   # NIST 标准参考态
  - unspecified                # 来源未说明工况
```

---

**问题 3：混合物组成表达不足**

`composition_value` 是一个标量 float，无法表达二元或多元混合物组成。即使 P0 不做混合物主表，condition_set 应当为混合物工况预留扩展点——因为多工况循环仿真很快就会需要它。

**建议**：将 `composition_value` + `composition_basis` 改为 FK 引用，或增加一个 `mixture_composition_json` 字段，以 JSON 存储 `{mol_id: mole_fraction}` 映射：

```yaml
- {name: mixture_composition_json, dtype: string, required: false}
```

并在 design_rules 注明："纯物质 observation 此字段为 null；混合物 observation 应填入 JSON"。

---

### 2.2 `property_observation_v2.yaml`

**问题 4：`value`（string）与 `value_num`（float）的二义性规则缺失**

两个字段同时存在是合理的（保留原始字符串 + 解析后数值），但草案没有规定：
- 何时允许 `value_num` 为空？
- ML 消费层（Stage 08）是否必须要求 `value_num` 非空？
- `"<0.001"` 这类区间值应该怎么处理？

**建议**：在 design_rules 中补充：

```
- value_num is required for all numeric properties consumed by ML layers.
- For inequality values (e.g. "<0.001"), value_num should store the numeric bound 
  and value_num_bound_type should indicate direction (upper/lower).
- If a source provides text-only values (e.g. "high", "moderate"), value_num remains null 
  and ml_use_status must be set to "excluded_non_numeric".
```

---

**问题 5：`normalization_rule_id` 是悬空引用**

`property_observation` 引用了 `normalization_rule_id`，但没有对应的 `normalization_rules` 表 schema。这意味着该字段在 P0 内无法被程序验证。

**建议**：要么在 schemas/drafts/ 中增加 `normalization_rules.yaml`，要么把 `normalization_rule_id` 标记为 `status: placeholder_for_v2`，明确不在 P0 验证。不能留悬空引用进入生产。

---

**问题 6：`quality_level` 词表未定义**

`quality_level` 出现在 property_observation、quantum result、cycle result 等多处，但始终没有受控词表。建议统一定义：

```
quality_level_vocabulary:
  - experimental_primary    # 一手实验，权威来源
  - experimental_secondary  # 文献转引实验值
  - computed_high           # 高精度量子计算（CCSD/DFT benchmark）
  - computed_standard       # 标准 DFT（B3LYP/6-311+G**）
  - eos_model               # 状态方程模型值（REFPROP/CoolProp）
  - estimated_group_contrib # 基团贡献法估算
  - ml_predicted            # ML 代理模型预测
  - unknown                 # 来源不明
```

---

### 2.3 `stage_run_manifest.yaml`

**问题 7：时间字段类型不当**

`started_at` 和 `finished_at` 定义为 `dtype: string`。Parquet 原生支持 timestamp 类型，使用字符串会在时序查询（如"上一次成功运行是什么时候"）时增加无谓的解析开销。

**建议**：改为 `dtype: timestamp[us, UTC]`，或在 design_rules 中规定标准格式（ISO 8601 with timezone）。

---

**问题 8：`run_id` 的作用域未定义**

`run_id` 是顶层构建的唯一 ID，但草案没有规定：完整重跑与局部 resume 是否共享同一 `run_id`？若 Stage 04 失败后 resume，新写入的 manifest 行是新 `run_id` 还是同一个？

**建议**：明确规定：
- 完整构建产生新的 `run_id`；
- Resume 从某个 stage 继续时，仍使用原 `run_id`，但 `started_at` 记录 resume 时间；
- 增加 `parent_run_id` 字段（可空），用于记录 resume 来源。

---

### 2.4 `research_task_readiness_rules.yaml`

**问题 9：规则存储为 Parquet 是反模式**

`data/lake/gold/research_task_readiness_rules.parquet` 是静态配置，不是数据产物。把它放在 gold 层并用 parquet 存储，意味着每次修改规则都要重新生成 parquet，且无法用 git diff 直观审查变更。

**建议**：规则文件保留为 YAML（`schemas/research_task_readiness_rules.yaml`），由 `validate.py` 直接读取。如果需要记录执行结果，再生成 `data/lake/gold/research_task_readiness_report.parquet`（结果 vs. 规则分离）。

---

**问题 10：必须属性 vs. 可选属性的语义缺失**

`required_properties_json` 是一个 JSON 字符串，但对于任何 ML 任务，属性通常分为"必须全部有"和"至少有 N 个"两种约束。`minimum_property_coverage` 只给出了一个全局覆盖率阈值，无法表达这种区分。

**建议**：将规则结构改为：

```yaml
property_requirements:
  must_have:       # 缺任意一个即不满足
    - gwp_100yr
    - odp
    - boiling_point
  should_have:     # 满足 minimum_coverage 即可
    - critical_temp
    - critical_pressure
    - heat_capacity_liquid
  minimum_coverage: 0.7
```

---

## 三、Phase 2 接口草案评审

### 3.1 量子计算接口

**问题 11：构象采样策略字段粒度不足**

`conformer_policy` 是一个单一字符串字段，但量子计算的构象选择对结果影响极大。建议拆分为：

```
conformer_generation_method: ETKDG | MMFF | xTB | user_provided
conformer_force_field: MMFF94 | UFF | xTB-GFN2
conformer_count: int
conformer_selection_criterion: lowest_energy | boltzmann_weighted | all
```

否则不同实现者会以不同方式解读 `conformer_policy`，破坏结果的可比性。

---

**问题 12：`method_family` + `model_chemistry` + `basis_set` 三字段存在命名混淆**

`method_family: DFT`、`model_chemistry: B3LYP`、`basis_set: 6-311+G**` 的分层是合理的，但 `method_family` 对于 post-HF 方法（CCSD(T)、MP2）定义不清：它们也属于某种 family，但 basis_set 的角色与 DFT 不同。

**建议**：统一为 `theory_level` 字符串（如 `B3LYP/6-311+G**/SMD`），并保留 `method_family` 作为分类枚举（DFT / HF / post-HF / semi-empirical / force-field），方便过滤查询。

---

### 3.2 循环仿真接口

**问题 13：`operating_point` 结构未定义**

`operating_point` 仅在 Request 中列出，但没有说明其内部结构。循环仿真的工况至少需要蒸发温度、冷凝温度、过冷度、过热度，这些都应当是结构化字段，而不是自由对象。

**建议**：在草案中增加 `CycleOperatingPoint` 的最小结构定义：

```
evaporating_temperature_c: float
condensing_temperature_c: float
subcooling_k: float
superheat_k: float
ambient_temperature_c: float (optional)
```

并规定这个结构会被哈希为 `operating_point_hash`（Result 中已有该字段）。

---

**问题 14：单组分 vs. 混合物场景未区分**

`fluid_name` 暗示可以填入混合物名称（如 R-410A），但没有说明如果 `mol_id` 指向一个纯物质，`fluid_name` 与 `mol_id` 如何对应。

**建议**：增加 `mixture_composition_json` 可选字段，并规定：纯物质场景 `mol_id` 必填；混合物场景 `mixture_composition_json` 必填，`mol_id` 填主组分或 null。

---

### 3.3 主动学习队列接口

**问题 15：缺少时间戳和版本字段**

`active_learning_queue` entry 没有 `created_at`、`updated_at`、`model_version`。不知道一个候选分子是在第几轮 AL 循环中被提名的，就无法事后分析主动学习策略是否有效。

**建议**：增加：
```
created_at: timestamp
campaign_id: string        # 标识同一批次的 AL 迭代
model_version: string      # 打分时使用的代理模型版本
expires_at: timestamp      # 可选，超时自动降优先级
```

---

**问题 16：`hard_constraint_status` 枚举值未定义**

这是一个关键过滤字段（安全约束是否满足），但草案没有定义合法值。

**建议**：枚举：`passed | failed | partially_passed | not_evaluated`

---

## 四、pipeline.py Stage 化方案评审

### 4.1 蓝图代码评审

**问题 17：`build_dataset_staged` 编排器无错误处理与 Manifest 写回**

当前蓝图的 orchestrator：

```python
result = spec.func(ctx)
results.append(result)
if result.status != "succeeded":
    break
```

失败时直接 break，但没有：
- 把失败状态写回 `stage_run_manifest`；
- 捕获 stage 函数内部的未处理异常；
- 区分"stage 显式返回 failed"和"stage 抛出异常"。

这是蓝图应当明确规范的，否则真实实现者会各自为政。

**建议**：在蓝图中补充 try/except 结构并写出 manifest 写回的接口占位：

```python
try:
    result = spec.func(ctx)
except Exception as exc:
    result = StageResult(stage_id=spec.stage_id, status="failed", notes=str(exc))
finally:
    _persist_stage_manifest(ctx, spec, result)  # 无论成功失败都写
```

---

**问题 18：`required_inputs` / `produced_outputs` 是装饰性字段**

`StageSpec` 声明了 `required_inputs` 和 `produced_outputs`，但 orchestrator 完全不使用它们做依赖检查。这意味着 Stage 05 可以在 Stage 01 尚未注册 inventory artifact 的情况下运行，只会在业务代码内部静默失败。

**建议**：在 orchestrator 的 stage 启动前增加 input guard：

```python
for input_name in spec.required_inputs:
    if ctx.get_artifact(input_name) is None:
        result = StageResult(stage_id=spec.stage_id, status="failed",
                             notes=f"Missing required input: {input_name}")
        break
```

或者至少在蓝图注释中明确"required_inputs 将在 PR-B 中接入 guard 检查"。

---

**问题 19：Stage 03 / Stage 04 的"全局 vs. 实体"边界模糊**

EPA SNAP 数据在草案里归入 Stage 03（全局源），但 SNAP 是按制冷剂型号（mol 级别）列出的。如果 Stage 03 提前运行并缓存了 SNAP 表，Stage 04 还需要从中按 mol_id 过滤——这部分逻辑跨阶段，需要明确哪个 stage 负责最终的"全局源 → entity 映射"步骤。

**建议**：在 Stage 03 的职责说明中补充："Stage 03 仅负责全局表的获取和缓存；entity 级别的 SNAP/EPA 行提取由 Stage 05 的 harmonize 步骤完成"。

---

**问题 20：`code_version` 无自动填充机制**

`stage_run_manifest` 记录了 `code_version`，但蓝图代码里没有任何机制（`git describe`、`importlib.metadata.version` 等）来自动填写它。这会导致生产跑出来的 manifest 里 `code_version` 全是 null。

**建议**：在 `stage00_init_run` 中加入：

```python
import subprocess
result = subprocess.run(["git", "describe", "--tags", "--always"], 
                        capture_output=True, text=True)
ctx.state["code_version"] = result.stdout.strip()
```

---

### 4.2 Stage 划分策略评审

**问题 21：Stage 07 过重**

`build_feature_and_recommendation_layers` 包含三个语义上独立的子任务：

1. 构建 `property_recommended`（数据层）
2. 计算 `structure_features`（计算密集型，依赖 RDKit）
3. 构建 `molecule_master` + `property_matrix`（聚合层）

当 structure_features 计算失败（例如某个分子 RDKit 解析失败）时，会一并阻塞 property_recommended 和 molecule_master 的生成。

**建议**：在 P0 范围内可以保持合并，但在 design_rules 注释中明确"Stage 07 内部的三个子步骤应具备独立失败处理，不应因单分子特征计算失败导致整个 Stage 失败"。并计划在 V1.5 拆分为 Stage 07a / 07b / 07c。

---

## 五、科研任务就绪性验证评审

### 5.1 任务定义完整性

**问题 22：TASK-01 属性名绑定到列名，而非 canonical_feature_key**

TASK-01 要求 `boiling_point_c`、`critical_temp_c` 等字段，但这些是 property_observation 里的 `property_name` 值（自由字符串），而不是 canonical 层的 `canonical_feature_key`。如果 NIST 返回的字段是 `boiling_point` 而某个手工来源是 `bp_celsius`，这个检查就会假阴性。

**建议**：所有 readiness 规则应绑定到 `canonical_feature_key`（由 governance bundle 统一定义），而不是原始 `property_name`。在验证文档中明确这一点。

---

**问题 23：缺少"降级策略"概念**

当某个任务未完全满足 readiness 时，应当区分：
- **硬失败**（Hard Fail）：缺少 GWP 这种核心约束，任务无法执行；
- **降级运行**（Degraded Run）：缺少某个 nice-to-have 属性，任务可执行但结果有偏；
- **警告**（Warning）：数据覆盖率低于 optimal 但高于 minimum。

当前草案只有 pass/fail 二值，不足以支持科研决策。

**建议**：在 validation 输出 JSON 中增加：

```json
{
  "task": "single_component_downselection",
  "status": "degraded",          // passed | degraded | failed
  "hard_failures": [],
  "warnings": ["gwp_100yr coverage: 72% (threshold: 80%)"],
  "degradation_impact": "GWP constraints may not apply to 28% of candidates"
}
```

---

**问题 24：缺少对"模型包含集"大小的最低验证**

`model_inclusion=yes` 的子集有多少分子是任务可执行的底线。5 个分子的 strict ML 和 500 个分子的 strict ML 在方法论上是完全不同的。

**建议**：在 readiness 规则 schema 中增加 `minimum_molecule_count: int` 字段。

---

## 六、整体范围与策略评审

### 6.1 旧数据迁移策略的缺失

草案第 6 节（过渡策略）说明"旧字段 temperature/pressure 在 P0 保留"，但没有回答：**现有 `property_observation` 中的数十条/百条 observation 如何逐步迁移到 `condition_set_id` 引用**？是手工标注、还是正则解析、还是 LLM 抽取？

这不是"P0 不做"可以回避的问题——如果不给出迁移路径，V1.5 结束时会存在新旧两套条件表达的双轨数据，Stage 05 的 harmonize 将永远带着这个双重逻辑。

**建议**：在 `p0_scope_and_exit_criteria.md` 中补充一节"条件字段迁移策略"，明确：
- 新写入的 observation 必须带 `condition_set_id`；
- 存量数据按 `condition_role` 分批回填（哪些可自动解析，哪些需人工）；
- 给出一个 backfill 脚本的交付时间节点。

---

### 6.2 数据版本化策略未覆盖

整个 P0 包没有提到"数据库版本如何迭代"。当 governance bundle 更新了某个属性的 canonical 值，或者 NIST 修正了一个 boiling_point，`model_ready.parquet` 应当怎么处理？当前 `stage_run_manifest` 只记录了 pipeline 运行，但没有记录"这次运行的数据库版本号是什么"。

**建议**：引入轻量的 `dataset_version` 概念（例如 `v1.5.0-20260423`），在 `stage_run_manifest` 和 `quality_report.json` 中同时记录，并规定：governance bundle 更新时触发 minor 版本升级，seed catalog 更新触发 patch 版本升级。

---

### 6.3 PR-B 的体量风险

PR-B（"把现有逻辑平移进 stage modules，保持行为不变"）涉及将整个 `pipeline.py` 重组，是三个 PR 中风险最高的一个。在一个科研项目中，"保持行为不变"的平移往往会暴露出原来隐式依赖的全局状态（DataFrame 传递、函数副作用）。

**建议**：PR-B 提交前增加一条强制验收标准：**对同一份种子数据，PR-B 前后 `model_ready.parquet` 的行数、列均值和 null 率必须完全一致（bit-for-bit 或统计等价）**，并把这个比对脚本作为 PR-B 的 CI 检查。

---

## 七、缺失但建议补充的内容

以下内容在 P0 包中完全缺失，建议在 P0 退出前或 V1.5 启动时补充：

| 缺失项 | 建议处理方式 |
|---|---|
| `canonical_feature_key` 的权威定义表 | 在 schemas/ 下增加 `canonical_feature_registry.yaml` |
| `normalization_rules` 表 schema | 补充 `schemas/drafts/normalization_rules.yaml` 或删除悬空引用 |
| `molecule_split_definition` 表 | 记录 scaffold split 的划分结果和策略，作为可追溯产物 |
| `source_manifest` 表 schema | 多处引用但未在 P0 包中提供草案 |
| 混合工质 `mixture_composition` 表预留 schema | 即使 P0 不做，应给出"到时候怎么扩展"的占位说明 |
| DFT 基准方法的统一规范 | 明确推荐使用哪个泛函/基组作为项目默认标准 |

---

## 八、优先级建议

按"落地前必须修复 vs. 可迭代改进"分级：

**必须在 P0 内修复（阻塞级）**

1. `condition_set_id` 生成规则 → 影响所有条件数据的去重和聚合
2. `condition_role` 受控词表 → 影响多工况过滤查询
3. Orchestrator 的异常捕获 + Manifest 写回 → 影响断点续跑的可靠性
4. `required_inputs` guard 检查 → 防止 Stage 间隐式依赖导致静默失败
5. Readiness 规则绑定 `canonical_feature_key` 而非 `property_name`

**P0 内应明确记录（非阻塞但需文档化）**

6. `value_num` / `value` 的使用规则
7. `quality_level` 受控词表
8. `run_id` 的 resume 语义
9. Stage 03 / Stage 04 的 EPA SNAP 归属说明
10. 条件字段存量迁移的路径规划

**V1.5 阶段改进**

11. Stage 07 的内部子步骤分离
12. ALQ entry 的时间戳和 campaign_id
13. 数据库版本化策略
14. `research_task_readiness_rules` 改为 YAML 配置而非 parquet

---

## 九、总结

P0 草案在方向上是正确的，工程判断是清醒的。它的最大价值在于**把一个可以无限膨胀的 V2 愿景，收束到一个边界清晰的 V1.5 过渡计划**——这是很多研究型数据库项目做不到的事。

主要风险集中在三处：
1. **Schema 层的语义未闭合**（condition_set_id 生成、condition_role 词表、quality_level 词表）；
2. **Pipeline 蓝图的工程健壮性不足**（异常处理、依赖 guard、版本记录）；
3. **过渡策略的存量数据迁移路径未给出**。

这三处如果不在 P0 退出前补齐，P0 的退出标准（5.2 schema 标准、5.3 工程标准）将无法真正达到，只是形式上通过而已。

---

*本评审报告对应 P0 包版本：`R-PhysGen-DB_P0_package`（2026-04-23 评审时状态）*
