# 科研任务就绪性验证草案（v3）

## 1. 目的

现有验证保证表结构、inventory 和质量门槛。但第五代制冷剂设计需要进一步回答：“这个数据集能不能支撑某个科研任务？”

P0 增加 `research_task_readiness`，用于输出每个任务的 `passed/degraded/failed` 状态。

## 2. 原则

- 所有属性引用必须使用 `canonical_feature_key`；
- readiness 规则来自 `schemas/research_task_readiness_rules.yaml`；
- 执行结果写入 `validation_report.json`，可选写入 `data/lake/gold/research_task_readiness_report.parquet`；
- 规则文件是 config，不是 gold 数据产物；
- 结果必须区分 hard failure、warning 和 degradation impact。

## 3. 状态语义

`failed`：

- molecule_count 小于 minimum_molecule_count；
- 任一 must_have 覆盖低于 required_coverage；
- require_source_traceability=1 但缺 source；
- allow_proxy_rows=0 但 proxy-only 行参与了 must_have；
- require_strict_layer=1 但来源不是 strict layer。

`degraded`：

- must_have 通过；
- should_have 平均覆盖或目标覆盖低于 minimum_should_have_coverage；
- 或非硬性质量指标不足。

`passed`：

- molecule_count、must_have、should_have、source、proxy、numeric/strict 约束全部满足。

## 4. 输出结构

```json
{
  "research_task_readiness": {
    "task01_single_component_downselection": {
      "task_name": "single_component_downselection",
      "status": "degraded",
      "molecule_count": 120,
      "minimum_molecule_count": 20,
      "source_layer": "property_recommended_canonical_or_legacy_recommended",
      "must_have_coverage": {
        "thermodynamic.normal_boiling_temperature": 1.0,
        "thermodynamic.critical_temperature": 1.0,
        "thermodynamic.critical_pressure": 0.95,
        "environmental.gwp_100yr": 0.70,
        "environmental.odp": 0.70
      },
      "should_have_coverage": {
        "safety.safety_group": 0.45,
        "safety.toxicity_class": 0.43
      },
      "hard_failures": [],
      "warnings": ["safety.safety_group below target coverage"],
      "degradation_impact": "Safety ranking is incomplete; use results only for thermodynamic/environmental pre-screening."
    }
  }
}
```

## 5. Validator 实施步骤

1. 读取 `canonical_feature_registry.yaml`；
2. 读取 `research_task_readiness_rules.yaml`；
3. 检查所有 feature key 是否已注册；
4. 按 source_layer 选择数据源；
5. 将 legacy property_name 通过 registry 映射到 canonical_feature_key；
6. 计算 molecule_count；
7. 计算 must_have 和 should_have 覆盖；
8. 检查 numeric、source、proxy、strict、quality_score；
9. 输出 status、hard_failures、warnings、degradation_impact。

## 6. 与现有 validate.py 的关系

新层不替代现有验证，而是挂在其后：

```text
schema/integration/inventory/quality gates
        ↓
canonical strict and review queue checks
        ↓
research_task_readiness
```

如果工程验证失败，不应宣称 readiness passed。

## 7. 任务定义摘要

任务定义以 YAML 为准，文档只解释语义：

- `single_component_downselection`：单组分候选筛选；
- `core_multitask_training`：promoted subset 多任务训练；
- `canonical_strict_ml`：只用 strict canonical slice；
- `phase2_cycle_seed`：为多工况循环试算选种子。

## 8. 降级建议

当任务 degraded 时，报告应给出可操作建议：

- 缺 GWP/ODP：先补环境标签，不建议做最终筛选；
- 缺安全分类：只可做热物性预筛；
- strict 集合过小：不可宣传为泛化 ML benchmark；
- 缺 condition_set：不可做多工况属性建模；
- proxy-only 占比高：结果只能作为 screening，不作为结论。
