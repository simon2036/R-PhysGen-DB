# P0 评审意见响应矩阵（v3）

本矩阵对应 `R-PhysGen-DB_P0_review.md` 中 24 项问题，并在 v2 修改基础上进一步闭合仍然存在的语义和工程缺口。

| # | 评审问题 | v3 处理 |
|---|---|---|
| 1 | `condition_set_id` 生成规则未定义 | 改为 `cond_ + sha256(canonical_json).hexdigest()[:20]`，新增 `condition_signature_json` 和 canonicalization 规则 |
| 2 | `condition_role` 缺词表 | 扩展到 12 项，补充 phase/unit/composition/normalization vocab |
| 3 | 混合物组成表达不足 | 保留 `mixture_composition_json`，新增 `mixture_composition_hash`，并新增 `mixture_composition.yaml` |
| 4 | `value` / `value_num` 二义性 | 新增 `value_num_lower/upper`、`value_parse_status`、interval 规则 |
| 5 | `normalization_rule_id` 悬空 | `normalization_rules.yaml` 从占位改为 P0 可验证 config，非空时验证 |
| 6 | `quality_level` 词表缺失 | 在 `property_observation_v2.yaml` 中给出扩展词表 |
| 7 | manifest 时间字段类型 | 保持 timestamp[us, UTC]，并增加 elapsed_s |
| 8 | `run_id` resume 语义 | 新增 `attempt_id` 和 `attempt_number`，解决同一 stage 多次尝试主键冲突 |
| 9 | readiness 规则不应是 Parquet | 保持 YAML config，结果另写 report |
| 10 | must/should 语义缺失 | `research_task_readiness_rules.yaml` 增加 machine-readable `rules` 列表 |
| 11 | 构象策略粒度不足 | Phase 2 文档拆分构象生成、力场、数量、选择准则 |
| 12 | 方法命名混淆 | 保留 `method_family`，增加 `theory_level` 作为完整字符串 |
| 13 | operating_point 未定义 | Phase 2 文档新增 `CycleOperatingPoint` 和 hash 规则 |
| 14 | 单组分/混合物未区分 | Phase 2 文档区分 mol_id、mixture_id、mixture_composition_json |
| 15 | ALQ 缺时间戳/版本 | Phase 2 文档新增 campaign/model/version/timestamps |
| 16 | hard_constraint_status 枚举缺失 | Phase 2 文档给出枚举 |
| 17 | orchestrator 无异常捕获/manifest 写回 | blueprint 改为 try/except/finally，异常也写 manifest |
| 18 | required_inputs 未使用 | blueprint 加 input guard，支持 file/logical artifact |
| 19 | Stage 03/04 SNAP 边界模糊 | pipeline 文档与 blueprint 明确 Stage 03 只缓存，Stage 05 映射 |
| 20 | code_version 未自动填充 | blueprint 用 git describe，dataset_version 用 VERSION 或日期 draft |
| 21 | Stage 07 过重 | blueprint 子步骤独立处理，文档规定 V1.5 拆 07a/b/c |
| 22 | readiness 绑定 property_name | v3 改用 namespaced canonical_feature_key，并改 registry 兼容 governance bundle |
| 23 | 缺降级策略 | validation 文档和 rules 输出均支持 passed/degraded/failed |
| 24 | 缺最低分子数验证 | 每条 readiness rule 均有 minimum_molecule_count |

## v3 相对 v2 的额外修正

1. 把 `canonical_feature_registry` 从简写 key 改为 namespaced key，避免与现有 governance bundle 的 `thermodynamic.normal_boiling_temperature` 等 key 脱节。
2. `stage_run_manifest` 增加 `attempt_id`，避免 resume 后同一 `run_id/stage_id` 无法记录多次尝试。
3. `research_task_readiness_rules.yaml` 不再只有 schema 描述，而是包含可直接消费的 `rules` 配置。
4. 补充 `mixture_composition.yaml` 与 `molecule_split_definition.yaml`，回应评审第七节中的缺失项。
5. 新增 `scripts/backfill_condition_set.py` 和 `scripts/pr_b_equivalence_check.py`，把文档承诺落成可执行草案。
