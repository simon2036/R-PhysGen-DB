# R-PhysGen-DB V1 建库与执行计划

## Summary

基于当前仓库现状，`methods` 中已经有较完整的方法论和一个演示型 Python 原型，但现有脚本会随机生成样本，输出的 CSV 基本为空壳，不能直接作为正式数据库基础。  
本轮按你刚确认的路线推进：**V1 只做数据库底座**、**开源数据源优先**、**采用“文件湖 + DuckDB 轻索引”**。目标是先把“可追溯、可扩展、可直接喂给 AI 模型”的单组分制冷剂数据库建起来，把主动学习闭环、REFPROP、DFT 高通量计算留成二期接口。

## 总计划

1. **冻结 V1 范围与数据边界**
   明确只收录单组分、中性、小分子候选制冷剂；优先 C/H/F/Cl/Br/I/O/N/S、C1-C6、已知制冷剂与低 GWP 候选。
2. **建立数据库分层与数据契约**
   按 `raw -> bronze/silver -> gold` 三层落地，定义统一字段、单位、条件、来源和质量等级。
3. **先做种子数据集，再做候选扩展**
   先收 25-40 个“锚点制冷剂”做高质量基线，再扩到 80-150 个公开可得候选分子。
4. **实现标准化、去重、冲突处理和质量控制**
   保证 E/Z、立体异构、R 编号、CAS、PubChem 标识可追踪；冲突数据不覆盖，只分级推荐。
5. **生成模型就绪数据层**
   输出结构特征、物性矩阵、标签缺失掩码、可信度权重和 scaffold split。
6. **预留二期闭环接口**
   为后续 REFPROP、DFT、循环模拟、主动学习回流预先定义接口，不在 V1 落地重计算。

## 逐步执行细化

### 1. 范围冻结与仓库骨架

- 建立统一目录约定：`data/raw`、`data/bronze`、`data/silver`、`data/gold`、`schemas`、`pipelines`、`docs`。
- 明确 V1 不做混合工质主表，只在 schema 中预留 `mixture_core` / `mixture_component` 扩展位。
- 把 `methods/refrigerant_data_project/refrigerant_data_pipeline.py` 定位为“参考原型”，不复用其随机数据生成逻辑。
- 输出：
  `project_scope`、目录规范、数据分层说明、受控词表草案。
- 完成标准：
  实现者不需要再决定“收什么、不收什么、数据落哪一层”。

### 2. 设计核心数据契约

- 主存储采用 **Parquet**；查询与联表采用 **DuckDB**。
- 定义以下核心表：
  `source_manifest`：来源、许可、抓取时间、checksum、本地路径、解析版本。
  `molecule_core`：`mol_id`、canonical/isomeric SMILES、InChI、InChIKey、formula、MW、charge、stereo/EZ 标记。
  `molecule_alias`：CAS、R-number、PubChem CID、synonyms crosswalk。
  `property_observation`：长表，保存每条观测值和条件。
  `property_recommended`：按来源优先级和规则选出的推荐值。
  `structure_features`：RDKit 描述符、指纹、元素计数、卤素计数、环数、双键数等。
  `model_dataset_index`：split、label mask、confidence、feature 引用。
- `property_observation` 必须强制包含：
  `property_name`、`value`、`unit`、`temperature`、`pressure`、`phase`、`source_type`、`source_name`、`method`、`uncertainty`、`quality_level`。
- 完成标准：
  任意一个新数据源都能按同一契约写入，不需要重新改表。

### 3. 构建 Wave 1 种子数据集

- 先人工确定 25-40 个锚点分子，覆盖 CFC、HCFC、HFC、HFO、天然工质、环状候选。
- 优先接入开源源：
  PubChem：结构、标识符、同义名。
  NIST WebBook：基础热物性。
  CoolProp：可计算热物性/EOS 代理值。
  IPCC/EPA/ASHRAE 公开表：GWP、ODP、安全等级。
- 每次抓取都先落原始层，再登记到 `source_manifest`。
- REFPROP 在 V1 只定义 adapter 接口，不假定本地可用。
- 完成标准：
  至少形成一套可追溯的“锚点库”，而不是只得到一张平铺总表。

### 4. 标准化与质量控制流水线

- 用 RDKit 做分子标准化：脱盐、规范化、canonical/isomeric 表达、InChI/InChIKey 生成。
- 去重规则：
  用 **full InChIKey** 区分真实实体；
  用 **first block** 做骨架聚类和重复审查；
  **E/Z 异构体必须保留**，例如 `R1234ze(E)` 与 `R1234ze(Z)` 不合并。
- 过滤规则：
  排除盐、金属、聚合物、明显不稳定结构、混合工质伪 SMILES。
- 数值 QC：
  `Tb < Tc`、汽化焓非负、单位可换算、ASHRAE 分类合法、GWP/ODP 范围合理。
- 冲突处理：
  所有观测值保留；
  另生成 `property_recommended`；
  冲突较大时打 `conflict_flag`。
- 完成标准：
  同一分子的来源冲突、异构问题、单位问题都能被系统化处理。

### 5. 生成模型就绪层

- 计算基础结构特征：
  MolWt、LogP、TPSA、QED、环数、双键数、卤素计数、Morgan/MACCS 指纹。
- 输出三类金层数据：
  `molecule_master`：实体与基础特征。
  `property_matrix`：面向建模的标签宽表。
  `model_ready`：SMILES/SELFIES、split、label mask、confidence、source coverage。
- 数据划分采用 **scaffold split**，而不是随机切分。
- 缺失标签不做简单均值填充，保留 mask，为后续多任务学习准备。
- 完成标准：
  下游 GNN/VAE/扩散模型可以直接读取，不需要再做二次清洗。

### 6. 文档、验证与二期接口

- 文档要写清：
  字段定义、单位标准、来源优先级、如何新增数据源、如何回溯原始证据。
- 二期只先定义接口，不实现重计算：
  `quantum_calculation`
  `cycle_simulation`
  `active_learning_queue`
- 后续如果你确认有 REFPROP 授权或算力环境，直接往现有 schema 上挂接，不推翻 V1。
- 完成标准：
  V1 交付后，新增数据源和进入闭环计算都属于增量扩展，不需要重做底座。

## Test Plan

- Schema 测试：每张 Parquet 表字段、类型、必填项、受控词表通过校验。
- 实体测试：`R32`、`R134a`、`R1234yf`、`R744`、`R717` 至少 5 个锚点分子能完整贯通 raw 到 gold。
- 去重测试：`R1234ze(E)` 与 `R1234ze(Z)` 必须分离保存。
- 单位测试：温度、压力、密度、焓、GWP 版本字段转换正确。
- 溯源测试：任一 `gold` 标签都能追溯到 `source_manifest` 和原始文件。
- 切分测试：train/val/test 之间无 scaffold 泄漏。
- 质量测试：覆盖率、缺失率、冲突率、异常率自动汇总成报告。

## Assumptions

- V1 只做**单组分制冷剂数据库底座**，不直接实现主动学习飞轮。
- “第五代制冷剂”在本项目中按**低 GWP、零/近零 ODP、高能效、安全受约束、可合成**的目标候选空间处理，不强依赖单一外部分类名单。
- 数据源按**开源优先**推进；REFPROP、商用数据库、DFT/MD 集群属于二期增强。
- 当前仓库中的现有脚本和 CSV 不作为正式数据真值，只作为方法参考与原型素材。
