# Excel `制冷剂数据库202603.xlsx` 简短报告

## 结论

- 这份 workbook 适合同时承担两件事：补当前数据库缺失标签，以及扩一批新的 `Tier D` inventory-only 候选。
- 本批已按保守口径落地：正式补 `Tb/Tc/Pc/ω/ΔvapH/ODP` 与新维度 `Zc`，不把未标明时间尺度的裸 `GWP` 和 `Hv[298K]` 直接并入主表。
- `2-热物性参考` 已按名字级弱证据处理：现有库只做唯一精确名称补充，workbook-only 条目只进入 staging，不直接进 `seed_catalog`。

## Workbook 结构判断

- `1-ODS八维数据库`: 775 行，适合作为结构化交叉校验和 `Zc` 来源。
- `1-1-NIST热物性实验数据-Aspen`: 776 行，是 `Tb/Tc/Pc/ω` 的主补库来源。
- `1-2-NIST热物性实验数据-蒸发潜热`: 776 行，是 `ΔvapH` 的主补库来源。
- `2-GWP和ODP`: 776 行，当前只正式吸收 `ODP`。
- `2-热物性参考`: 1016 行，经拆长后为 2058 条 name-only 参考记录。
- `1-NIST付费数据`: 2 行，但只有 1 个唯一名称，格式异常且证据链不完整，本批只做风险提示，不自动入库。

## 对现有数据库的补充

- 新写入 observation 行数: `136`。
- `acentric_factor` 新补充 `16` 行。
- `boiling_point_c` 新补充 `23` 行。
- `critical_compressibility_factor` 新补充 `48` 行。
- `critical_pressure_mpa` 新补充 `16` 行。
- `critical_temp_c` 新补充 `15` 行。
- `odp` 新补充 `18` 行。
- 裸 `GWP` 命中 `47` 行，但因时间尺度未明确，当前只保留在报告分析里。
- `Hv[298K]` 命中 `158` 行，但与现有 `vaporization_enthalpy_kjmol` 口径不同，当前不并表。

## 新维度与扩库存

- 已正式新增 `critical_compressibility_factor` (`Zc`) 为 numeric property，但不进入模型目标集合。
- 结构化 workbook-only 条目合并后共有 `741` 条候选视图。
- 其中导出到 generated `Tier D` 候选补充文件的共有 `568` 条。
- 这些 workbook-only 候选额外贡献了 `1127` 行 workbook property observations。
- 因现库别名已命中而跳过 `81` 条，因结构重复而跳过 `39` 条。
- 其中 `200` 行候选属性来自 `2-热物性参考` 的精确名字桥接回填。
- 上述桥接里有 `160` 行来自已解析 alias 的二次增强。
- 另有 `2` 行因同一候选属性存在冲突值而被跳过。
- 另外过滤掉 `11` 行超范围 `ODP` 值，避免把明显异常值写入主库。
- `2-热物性参考` workbook-only 名称已写入 staging: `1757` 行 / `936` 个唯一名称。

## 风险说明

- `2-热物性参考` 只有名字，没有 CAS/SMILES；因此它适合做现有库补缺或后续人工/程序解析前的暂存，不适合直接当作结构化 seed 源。
- `1-NIST付费数据` 当前表内重复和结构都不稳定，自动入库风险高，本批不使用。
- generated `Tier D` 候选默认全部按 `candidate` 处理，不自动提升为 `refrigerant`。
