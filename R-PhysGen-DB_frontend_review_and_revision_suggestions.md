# R-PhysGen-DB 最新前端评审与修改建议

评审对象：`前端claudeclaudedesignclaudedesignv1.zip`  
评审目标：判断当前前端是否适合作为 R-PhysGen-DB 的公开学术数据库网站，并给出可执行的修改路线。

---

## 1. 总体结论

当前版本的视觉方向是正确的，已经从“普通网页”进入了“科研数据产品 / 数据库控制台”的气质。整体风格专业、现代，具备学术数据库所需的若干关键概念，例如 canonical layer、source priority、review decisions、strict ML-ready subset、property observations 等。

但它目前更像一个高保真静态 Demo 或内部数据治理控制台，还不是可上线的公开学术数据库前端。最大问题不是美观，而是工程形态、真实数据接入、可引用性、可访问性和核心检索能力还没有闭环。

综合评分如下：

| 维度 | 评分 | 说明 |
|---|---:|---|
| 视觉完成度 | 8 / 10 | 风格专业，现代感较强 |
| 学术数据库气质 | 7 / 10 | 有数据治理和 provenance 意识，但公开入口不足 |
| 数据交互能力 | 5 / 10 | 有浏览雏形，但筛选、导出、对比尚未闭环 |
| 工程可维护性 | 3 / 10 | CDN + Babel runtime，不适合生产 |
| 上线可信度 | 3 / 10 | 存在假状态、占位功能、统计与样本不一致等问题 |

核心判断：

> 保留当前视觉风格，但不要继续把这个版本作为最终工程扩展。建议把它作为设计原型，迁移到 Vite 或 Next.js + TypeScript 的正式前端工程中。

---

## 2. 当前版本文件结构观察

压缩包内主要包含以下文件：

```text
R-PhysGen-DB.html
components.jsx
data.js
page_dashboard.jsx
page_molecules.jsx
page_rest.jsx
styles.css
```

当前实现方式是：在 HTML 中通过 CDN 加载 React、ReactDOM 和 Babel，然后在浏览器端实时编译 JSX。该方式适合快速展示，但不适合作为正式上线工程。

---

## 3. 当前版本做得好的地方

### 3.1 信息架构方向正确

当前前端没有停留在“搜索框 + 表格”的简单数据库页面，而是引入了：

```text
dashboard
molecules
mixtures
canonical layer
review decisions
regulatory
cycle
active learning
sources
```

这些模块说明当前设计已经理解 R-PhysGen-DB 不是一个普通物性表，而是包含数据治理、来源优先级、标准化推荐值、模型可用子集等结构化数据库能力。

### 3.2 视觉语言较专业

当前 `styles.css` 使用了 warm-neutral light theme 和 dark theme，配合 serif 标题、mono 数值、紧凑表格、quality bar、tier pill 等元素，整体更像现代科研数据平台，而不是老旧 HTML 数据库。

建议保留当前视觉方向。

### 3.3 分层语义表达清楚

页面中多次出现以下概念：

```text
silver → gold → strict
property_observation
property_recommended_canonical
ML-ready
source priority rank
canonical feature key
```

这些概念对学术数据库非常重要。它们让用户知道数据不是随意展示的，而是经过来源治理、质量评估和标准化选择的。

### 3.4 分子详情页骨架正确

分子详情页已经包含：

```text
基础身份信息
SMILES
InChIKey
CAS
canonical properties
observations
regulatory
provenance tabs
```

这是学术数据库条目页应有的基本结构。

### 3.5 适合作为内部数据治理控制台

如果目标是给项目组、审稿人或数据维护者展示数据治理流程，当前版本已经有很好的基础。但如果目标是公开学术数据库网站，还需要补齐公开访问者的检索、筛选、引用、导出和对比闭环。

---

## 4. 当前最大问题：更像内部控制台，而不是公开学术数据库首页

当前首页名为：

```text
Refrigerant database overview
```

首屏展示的是：

```text
resolved molecules
property observations
canonical recommended
model dataset
```

这些信息对内部团队有价值，但公开数据库用户的第一需求通常是：

```text
我想查某个制冷剂
我想筛选 GWP < 150 且 ODP = 0 的候选物
我想比较 R-32、R-1234yf、R-290
我想知道某个数值来自哪里
我想引用或下载数据
我想通过 API 查询
```

因此，当前首页应该拆分为两个角色。

### 4.1 公开门户首页

建议新增公开首页，导航结构如下：

```text
Home
Browse Database
Compare
Dataset Versions
API & Downloads
Methods
Submit / Contribute
```

公开首页首屏建议为：

```text
R-PhysGen-DB
Traceable refrigerant property database for next-generation low-GWP candidates.

[Search by refrigerant, formula, CAS, property range]
[Browse database] [Compare candidates] [API docs]
```

首页应优先展示：

```text
统一搜索框
数据库简介
数据规模
数据版本
Citation / DOI 入口
快速筛选入口
Featured refrigerants
API / Download 入口
```

### 4.2 内部治理控制台

当前 Dashboard 更适合放到内部治理区域：

```text
Data Governance
  Canonical Layer
  Review Decisions
  Sources & Provenance
  Regulatory
  Active Learning
  Cycle Operating Points
```

建议将现有首页改为：

```text
/dashboard
```

或：

```text
/admin/overview
```

---

## 5. 必须优先修改的问题

### 5.1 改造工程形态

当前 HTML 中通过 CDN 加载 React、ReactDOM 和 Babel，并通过 `type="text/babel"` 加载 JSX。这种方式不适合生产：

```html
<script src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
<script src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
<script src="https://unpkg.com/@babel/standalone/babel.min.js"></script>
```

主要问题：

```text
依赖外网 CDN
浏览器端实时编译 JSX
无法 tree-shaking
无法类型检查
无法构建优化
不利于 CSP 和安全策略
不利于 CI/CD
```

建议迁移为正式工程：

```text
frontend/
  package.json
  index.html
  src/
    main.tsx
    App.tsx
    components/
    pages/
    data/
    lib/
    styles/
```

技术栈建议：

```text
Vite + React + TypeScript
```

如果需要 SSR、API routes、动态路由和数据版本页，则建议：

```text
Next.js + TypeScript
```

---

### 5.2 修复 viewport

当前 HTML 中使用：

```html
<meta name="viewport" content="width=1280"/>
```

这会强制移动端按 1280px 宽度渲染，导致响应式规则基本失效。

应修改为：

```html
<meta name="viewport" content="width=device-width, initial-scale=1"/>
```

并补充移动端样式：

```css
@media (max-width: 760px) {
  .app {
    grid-template-columns: 1fr;
  }

  .sidebar {
    position: static;
    height: auto;
    border-right: 0;
    border-bottom: 1px solid var(--border);
  }

  .topbar {
    padding: 0 16px;
  }

  .topbar-right {
    display: none;
  }

  .page {
    padding: 24px 16px 48px;
  }

  .hero-panel {
    grid-template-columns: 1fr;
  }

  .struct-box {
    width: 100%;
  }
}
```

---

### 5.3 删除或替换“假连接状态”

当前界面中出现类似：

```text
DuckDB · r_physgen_v2.duckdb
87 tests passing
```

但当前包里没有真实 DuckDB 接入，也没有测试文件或 CI 状态读取能力。

这会削弱学术数据库的可信度。建议在 Demo 阶段改成：

```text
Demo snapshot · local mock data
```

或：

```text
Prototype dataset · representative subset
```

只有在真正接入 DuckDB 并且前端可以确认连接状态后，才显示：

```text
DuckDB connected · r_physgen_v2.duckdb
```

同理，`87 tests passing` 必须删除，除非仓库里真的存在测试并且前端能读取 CI 状态。

---

### 5.4 修复统计数据与实际样本不一致

当前 `data.js` 中实际只有少量 demo 数据，但 `stats` 中显示较大规模数字，例如：

```text
5598 resolved molecules
15689 property observations
1389 canonical recommended
```

如果表格只能展示几十条数据，用户会误以为数据缺失或系统错误。

建议二选一：

#### 方案 A：明确标注 Demo 数据

```text
Showing representative demo slice: 35 of 5,598 molecules
```

#### 方案 B：统计数据从真实数据计算

```js
const stats = {
  molecules: db.mols.length,
  observations: db.observations.length,
  canonical: db.canonical.length,
  mixtures: db.mixtures.length
};
```

正式上线时建议采用方案 B。

---

### 5.5 修复搜索功能与 placeholder 不一致

当前搜索框 placeholder 写的是：

```text
Search molecules, mixtures, CAS, InChIKey, property keys…
```

但实际搜索只过滤了 molecule fields：

```text
mol_id
r_name
formula
cas
inchikey
family
```

没有搜索 mixtures，也没有搜索 property keys。

建议二选一。

#### 方案 A：收窄 placeholder

```text
Search molecule name, CAS, formula, InChIKey…
```

#### 方案 B：实现真正的全局搜索索引

```js
const buildSearchIndex = (db) => [
  ...db.mols.map(m => ({
    type: "molecule",
    id: m.mol_id,
    title: m.r_name,
    subtitle: `${m.formula} · ${m.cas || ""}`,
    haystack: [
      m.mol_id,
      m.r_name,
      m.formula,
      m.cas,
      m.inchikey,
      m.family,
      ...(m.applications || [])
    ].filter(Boolean).join(" ").toLowerCase()
  })),

  ...db.mixtures.map(mx => ({
    type: "mixture",
    id: mx.mixture_id,
    title: mx.mixture_name,
    subtitle: `Mixture · GWP ${mx.gwp}`,
    haystack: [
      mx.mixture_id,
      mx.mixture_name,
      mx.ashrae_class,
      mx.app
    ].filter(Boolean).join(" ").toLowerCase()
  })),

  ...db.properties.map(p => ({
    type: "property",
    id: p.key,
    title: p.name,
    subtitle: `${p.group} · ${p.unit}`,
    haystack: [
      p.key,
      p.name,
      p.group,
      p.unit
    ].filter(Boolean).join(" ").toLowerCase()
  }))
];
```

---

## 6. 数据库浏览页修改建议

当前 `Molecules` 页有 Tier、Model、Family 筛选，表格列也比较合理。但对标专业学术数据库，还需要更完整的筛选和导出能力。

### 6.1 增加范围筛选

建议支持：

```text
GWP min / max
ODP min / max
NBP / K min / max
Tc / K min / max
Pc / MPa min / max
MW min / max
Safety class
Family
Tier
Model inclusion
Source coverage
Strict dataset inclusion
```

### 6.2 改成左侧 Facet Sidebar

当前顶部 chip 筛选更像轻量 dashboard。专业数据库建议采用：

```text
左侧：Facet filters
右侧上方：Search + Sort + Column settings + Export
右侧主体：Data table
右侧底部：Pagination / selected rows / compare action
```

### 6.3 表格应支持的能力

建议增加：

```text
列显隐
单位切换：K / °C / MPa / kPa
保存查询
复制当前查询 URL
批量加入 Compare
CSV 导出
JSON 导出
BibTeX 导出
分页或虚拟滚动
```

### 6.4 实现 CSV 导出

当前 CSV 按钮如果只是视觉按钮，需要补上实际导出功能：

```js
function toCsv(rows) {
  const columns = [
    "mol_id",
    "r_name",
    "formula",
    "cas",
    "family",
    "tier",
    "mw",
    "nbp",
    "tcrit",
    "gwp",
    "odp",
    "ashrae"
  ];

  const escape = (v) => {
    if (v === null || v === undefined) return "";
    const s = String(v);
    return /[",\n]/.test(s) ? `"${s.replaceAll('"', '""')}"` : s;
  };

  return [
    columns.join(","),
    ...rows.map(row => columns.map(c => escape(row[c])).join(","))
  ].join("\n");
}

function downloadText(filename, text, mime = "text/plain") {
  const blob = new Blob([text], { type: mime });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}
```

按钮示例：

```jsx
<button
  className="btn sm ghost"
  onClick={() => downloadText(
    "r-physgen-molecules.csv",
    toCsv(rows),
    "text/csv"
  )}
>
  <Icon name="download" size={12}/> CSV
</button>
```

---

## 7. 分子详情页修改建议

详情页当前结构较好，但还需要加强“可引用、可追溯、可复现”。

### 7.1 替换 2D structure placeholder

当前显示：

```text
2D STRUCTURE · placeholder
```

这会显著削弱公开数据库的专业感。

建议：

```text
优先方案：使用 RDKit.js 根据 SMILES 生成 SVG
备选方案：后端生成结构图，前端直接展示 SVG / PNG
最低限度：如果没有结构图，显示 structure unavailable，而不是 placeholder
```

### 7.2 补齐按钮真实功能

当前类似以下按钮不应只是视觉元素：

```text
Copy InChIKey
PubChem
NIST WebBook
Download record
```

建议实现：

```jsx
<button
  className="btn sm"
  onClick={() => navigator.clipboard.writeText(m.inchikey)}
>
  <Icon name="copy" size={12}/> Copy InChIKey
</button>

<button
  className="btn sm"
  onClick={() => window.open(
    `https://pubchem.ncbi.nlm.nih.gov/#query=${encodeURIComponent(m.inchikey)}`,
    "_blank"
  )}
>
  <Icon name="external" size={12}/> PubChem
</button>

<button
  className="btn sm primary"
  onClick={() => downloadText(
    `${m.mol_id}.json`,
    JSON.stringify(m, null, 2),
    "application/json"
  )}
>
  <Icon name="download" size={12}/> Download record
</button>
```

### 7.3 增加 Citation 面板

每个分子条目页应有引用区：

```text
Recommended citation
Dataset version
Snapshot date
Record ID
Canonical query URL
Source references
Export as JSON
Export as BibTeX
```

示例文案：

```text
R-PhysGen-DB contributors. R-1234yf canonical property record.
R-PhysGen-DB v1.5.0-draft, snapshot 2026-04-24.
Record: MOL_R1234yf.
```

### 7.4 Canonical property card 应支持来源展开

当前 canonical card 只显示：

```text
selected_source_name
rank
```

建议增加：

```text
source title
source type
DOI / URL
retrieved date
method
temperature / pressure condition
quality score explanation
conflict / divergence notes
```

---

## 8. 数据字典问题

当前 canonical 数据里存在以下 key：

```text
structure.tfa_risk_proxy
structure.synthetic_accessibility
```

但 `properties` 字典里没有注册这两个 key。结果是详情页会把它们归到 `Other`，property registry 也无法完整解释这些字段。

建议补充：

```js
{
  key: "structure.tfa_risk_proxy",
  group: "Structural",
  name: "TFA formation risk proxy",
  unit: "class",
  strict: false
},
{
  key: "structure.synthetic_accessibility",
  group: "Structural",
  name: "Synthetic accessibility score",
  unit: "score",
  strict: false
}
```

并增加启动时校验函数：

```js
function validateDb(db) {
  const propKeys = new Set(db.properties.map(p => p.key));

  const missing = db.canonical
    .map(c => c.canonical_feature_key)
    .filter(k => !propKeys.has(k));

  return [...new Set(missing)];
}
```

正式工程中，这类校验应进入测试流程。

---

## 9. 可访问性修改建议

当前有不少交互元素不是语义化控件。例如：

```text
sidebar nav item 使用 div onClick
table row 使用 tr onClick
breadcrumb 使用 a onClick 但没有 href
```

这会导致键盘用户无法正常操作，也不利于屏幕阅读器。

### 9.1 侧边栏改为 button

```jsx
<button
  type="button"
  className={"nav-item " + (page === it.id ? "active" : "")}
  onClick={() => setPage(it.id)}
  aria-current={page === it.id ? "page" : undefined}
>
  <Icon name={it.icon}/>
  <span>{it.label}</span>
  {it.badge && <span className="nav-badge">{it.badge}</span>}
</button>
```

### 9.2 表格行增加明确详情按钮

```jsx
<td>
  <button
    type="button"
    className="btn sm ghost"
    onClick={(e) => {
      e.stopPropagation();
      setSelectedMol(m.mol_id);
      setPage("molecule");
    }}
  >
    View
  </button>
</td>
```

### 9.3 排序表头增加 aria-sort

```jsx
<th aria-sort={active ? (desc ? "descending" : "ascending") : "none"}>
```

---

## 10. URL 路由问题

当前页面状态保存在 `localStorage` 中，例如：

```text
rpg-page
rpg-mol
```

这会导致用户无法分享具体条目链接，也无法直接打开查询结果页面。

学术数据库必须支持可分享 URL，例如：

```text
/compound/MOL_R1234yf
/database?q=R-1234yf
/compare?ids=MOL_R32,MOL_R1234yf,MOL_R290
```

推荐正式路由结构：

```text
/
 /database
 /molecule/:mol_id
 /mixture/:mixture_id
 /compare
 /sources
 /docs/api
 /admin/dashboard
```

如果继续使用纯静态单页，也至少应使用 hash route：

```text
#/molecules
#/molecule/MOL_R1234yf
#/database?q=R-32
```

---

## 11. 视觉进一步优化建议

当前 UI 已经专业，但偏“数据治理后台”。如果希望更像公开学术数据库并且显得前沿，可做以下调整。

### 11.1 首页增强 Hero

建议首页首屏采用：

```text
R-PhysGen-DB
Traceable refrigerant property database for next-generation low-GWP candidates.

[Search by refrigerant, formula, CAS, property range]
[Browse database] [Compare candidates] [API docs]
```

### 11.2 增加克制的科学视觉元素

可加入：

```text
分子骨架线
相图等高线
温压曲线
低透明度网格
数据点云背景
```

注意不要做成过度 neon cyberpunk，否则会削弱学术可信度。

### 11.3 品牌色系统

建议建立统一品牌色：

```css
--brand-blue: oklch(55% 0.12 220);
--brand-cyan: oklch(72% 0.14 205);
--brand-green: oklch(68% 0.13 150);
--brand-amber: oklch(72% 0.14 75);
```

但为了兼容较老浏览器，建议提供 fallback：

```css
--accent-fallback: #2b7cbf;
--accent: #2b7cbf;
--accent: oklch(52% 0.11 205);
```

---

## 12. 增加 Compare 功能

当前版本缺少真正的候选物对比功能，但这是制冷剂数据库非常关键的入口。

建议新增页面：

```text
Compare candidates
```

支持选择 2–6 个分子，并对比：

```text
基础信息：formula / family / safety / GWP / ODP
热物性：NBP / Tc / Pc / MW
循环性能：COP / volumetric capacity
来源质量：quality score / strict / proxy / conflict
```

浏览页每行增加：

```jsx
<button
  type="button"
  className="btn sm ghost"
  onClick={(e) => {
    e.stopPropagation();
    addToCompare(m.mol_id);
  }}
>
  Compare
</button>
```

底部出现 compare drawer：

```text
3 selected: R-32, R-1234yf, R-290
[Compare now] [Clear]
```

---

## 13. 数据层重构建议

当前所有数据挂在：

```text
window.DB
```

这适合 demo，但正式工程建议改成 typed data service。

推荐结构：

```text
src/
  data/
    demo-db.ts
  lib/
    db.ts
    search.ts
    export.ts
    units.ts
    validation.ts
  types/
    compound.ts
    property.ts
    source.ts
```

类型示例：

```ts
export type Molecule = {
  mol_id: string;
  r_name: string;
  family: string;
  formula: string;
  mw: number | null;
  smiles?: string;
  inchikey?: string;
  cas?: string;
  tier: "A" | "B" | "C" | "D";
  model_inclusion: "yes" | "no";
  status: "resolved" | "candidate" | "deprecated";
  odp?: number | null;
  gwp?: number | null;
  ashrae?: string | null;
  tox?: string | null;
  nbp?: number | null;
  tcrit?: number | null;
  pcrit?: number | null;
  applications?: string[];
};
```

数据读取建议分三层：

```text
demo data
真实 JSON / CSV 数据适配
后端 API / DuckDB 查询
```

不要让 React 组件直接关心底层数据来自 CSV、Parquet、DuckDB 还是 API。组件只消费统一 typed data。

---

## 14. 性能建议

当前表格只渲染少量 demo 数据，所以暂时没有明显问题。但如果未来需要展示：

```text
5598 molecules
15689 observations
14017 recommended properties
```

直接 `.map()` 全量渲染表格会出现卡顿。

建议正式工程使用：

```text
TanStack Table
TanStack Virtual
服务端分页或前端虚拟滚动
列级筛选
排序状态入 URL
```

最低限度也应实现分页：

```js
const pageSize = 100;
const pageRows = rows.slice(page * pageSize, (page + 1) * pageSize);
```

---

## 15. 语言和命名建议

当前页面全英文。如果目标用户包括国内项目团队、中文评审或中文论文读者，建议支持中英双语。

推荐策略：

```text
中文界面 + 英文数据术语保留
```

例如：

```text
数据库浏览 / Database Explorer
分子条目 / Molecule Record
标准化属性 / Canonical Properties
来源与溯源 / Sources & Provenance
模型可用子集 / ML-ready strict subset
```

注意：不要把 `canonical` 翻译得过于口语化。建议使用：

```text
标准化推荐值
推荐规范值
canonical recommended value
```

---

## 16. 修改优先级

### 第一轮：可信上线基础

优先处理：

```text
修复 viewport
删除或替换假状态：87 tests passing、DuckDB connected
实现 Copy / PubChem / NIST / Download / CSV 按钮
补齐 property dictionary 缺失 key
把 hard-coded 统计改成来自数据计算，或明确标注 demo slice
增加真实公开首页
```

### 第二轮：数据库核心能力

继续补齐：

```text
高级筛选：GWP / ODP / Tc / NBP / MW / safety / source
Compare 页面
详情页 citation 面板
导出 JSON / CSV / BibTeX
URL 路由和可分享查询
真实数据适配层
```

### 第三轮：正式工程化

最后完成：

```text
迁移到 Vite 或 Next.js
TypeScript 类型
模块化组件
测试与数据校验
虚拟化表格
CI 构建
可访问性修复
API 文档页
```

---

## 17. 建议保留与建议重做

### 17.1 建议保留

```text
整体色彩系统
light / dark theme
sidebar 的内部控制台风格
dashboard 的 pipeline 表达
molecule detail 的 tabs 结构
canonical property card
quality bar / tier pill / safety pill
source provenance 思路
```

### 17.2 建议重做

```text
首页
路由系统
数据接入层
浏览页筛选体系
导出功能
详情页结构图和引用区
所有按钮的真实交互
移动端响应式
可访问性语义
```

---

## 18. 最关键建议

当前版本已经有了“专业数据库控制台”的骨架和审美，但还没有形成“公开学术数据库网站”的产品闭环。

下一步不要继续堆页面，而应该先打通以下六个核心闭环：

```text
首页入口
检索浏览
条目详情
候选物对比
引用溯源
数据导出 / API
```

只要这六个闭环打通，R-PhysGen-DB 的前端就会从“好看的 Demo”升级为“可信、可用、可引用的学术数据库平台”。
