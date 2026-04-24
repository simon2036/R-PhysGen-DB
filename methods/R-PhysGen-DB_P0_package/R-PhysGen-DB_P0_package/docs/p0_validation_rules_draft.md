# 科研任务就绪性验证草案（P0 Draft）

## 1. 为什么要加这层验证

当前 `validate.py` 已经覆盖了：

- schema checks
- integration checks
- inventory checks
- quality gate checks

这对数据库工程足够好，但对科研任务仍不够。
因为“表存在”不等于“可用于第五代制冷剂设计任务”。

P0 建议新增一层：

- `research_task_readiness`

它回答的是：
- 这套数据能否做单组分候选 down-selection？
- 能否做多任务核心标签训练？
- 能否做 canonical strict ML slice？
- 能否支撑下一步的 cycle / quantum 增量接入？

## 2. 建议定义的任务

> **重要**：所有 readiness 规则中的属性引用必须绑定到 `canonical_feature_key`
> （由 `canonical_feature_registry.yaml` 统一定义），而不是原始 `property_name`。
> 这确保了跨来源的属性一致性匹配。

### TASK-01 `single_component_downselection`
目标：
用于单组分候选的约束筛选。

must_have（缺任一即失败）:
- mol_id
- boiling_point
- critical_temperature
- critical_pressure
- gwp_100yr（或版本化 GWP）
- odp

should_have（满足 minimum_coverage 即可）:
- ashrae_safety_class
- molecular_weight
- density
minimum_coverage: 0.7
minimum_molecule_count: 20

### TASK-02 `core_multitask_training`
目标：
训练 V1 当前的核心多任务属性预测器。

must_have（缺任一即失败）:
- mol_id
- boiling_point
- critical_temperature
- molecular_weight

should_have（满足 minimum_coverage 即可）:
- gwp_100yr
- odp
- ashrae_safety_class
- viscosity
- thermal_conductivity
minimum_coverage: 0.75
minimum_molecule_count: 50

条件要求：
- 在 promoted subset 内
- 标签必须 numeric / categorical 可消费
- 不允许无来源推荐值
- 对 proxy 行有明确策略

### TASK-03 `canonical_strict_ml`
目标：
使用 canonical strict 层进行高可信度 ML。

must_have（缺任一即失败）:
- mol_id
- boiling_point
- critical_temperature
- critical_pressure

should_have（满足 minimum_coverage 即可）:
- gwp_100yr
- odp
- ashrae_safety_class
minimum_coverage: 0.8
minimum_molecule_count: 30

条件要求：
- 来自 `property_recommended_canonical_strict`
- 满足 readiness rule
- 不使用开放 review queue 中的行

### TASK-04 `phase2_cycle_seed`
目标：
为后续多工况循环试算提供候选种子。

must_have（缺任一即失败）:
- mol_id
- boiling_point
- critical_temperature
- molecular_weight

should_have（满足 minimum_coverage 即可）:
- gwp_100yr
- odp
- viscosity
minimum_coverage: 0.65
minimum_molecule_count: 10

条件要求：
- identity 完整
- CoolProp / EOS source 状态明确
- 不处于明确的 structure ambiguity

## 2.5 降级策略

当某个任务未完全满足 readiness 时，应区分三个级别：

- **通过（passed）**：所有 must_have 和 should_have 要求均满足
- **降级运行（degraded）**：must_have 全部满足，但 should_have 覆盖率低于阈值
- **失败（failed）**：至少一个 must_have 属性缺失

降级场景下任务仍可执行，但结果有偏。验证输出应包含降级影响评估：

```json
{
  "task": "single_component_downselection",
  "status": "degraded",
  "hard_failures": [],
  "warnings": ["gwp_100yr coverage: 72% (threshold: 80%)"],
  "degradation_impact": "GWP constraints may not apply to 28% of candidates"
}
```

## 3. 建议新增表

建议新增：
- `research_task_readiness_rules`
- （可选）`research_task_readiness_report`

## 4. 验证规则建议

### 4.1 completeness
按任务定义必需属性集合，检查 coverage。

### 4.2 source eligibility
检查推荐值是否来自允许的 source type / quality level。

### 4.3 proxy policy
检查某任务是否允许 proxy-only 行进入。

### 4.4 strict compatibility
检查 strict 层与任务定义是否一致。

### 4.5 split eligibility
检查任务是否要求 `model_inclusion=yes`、是否只能在 promoted subset 上运行。

## 5. 输出形式建议

建议在 `validation_report.json` 里增加：

```json
{
  "research_task_readiness": {
    "single_component_downselection": {
      "task": "single_component_downselection",
      "status": "passed|degraded|failed",
      "molecule_count": 125,
      "minimum_molecule_count": 20,
      "must_have_coverage": {
        "mol_id": 1.0,
        "boiling_point": 1.0,
        "critical_temperature": 1.0,
        "critical_pressure": 0.95,
        "gwp_100yr": 0.92,
        "odp": 0.88
      },
      "should_have_coverage": {
        "ashrae_safety_class": 0.85,
        "molecular_weight": 0.98,
        "density": 0.80
      },
      "should_have_threshold": 0.7,
      "hard_failures": [],
      "warnings": ["odp coverage: 88% (required: 100%)"],
      "degradation_impact": "ODP constraints may not apply to 12% of candidates"
    },
    "core_multitask_training": {...},
    "canonical_strict_ml": {...},
    "phase2_cycle_seed": {...}
  }
}
```

## 6. 与现有验证的关系

- 现有验证继续保留；
- 新验证是更上层的任务视角，不替代 schema / integration checks；
- 二者一起构成“数据库工程可用 + 科研任务可用”的双层保证。