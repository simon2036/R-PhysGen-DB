#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多尺度物理信息嵌入的制冷剂分子设计
数据收集与预处理完整实验流程

基于论文《多目标约束下的制冷剂分子设计：从代际演化到 AI 驱动范式》
附录方向一：基于多尺度物理信息嵌入的多目标生成模型优化

作者：Research Assistant
日期：2026-04-19
"""

import numpy as np
import pandas as pd
from typing import Dict, List
import json
import warnings
warnings.filterwarnings('ignore')

# 可视化设置
import seaborn
seaborn.set_style("whitegrid")
from mplfonts import use_font
use_font('Noto Sans CJK SC')
import matplotlib.pyplot as plt
plt.rcParams['font.family'] = "sans-serif"
plt.rcParams['font.sans-serif'] = "Noto Sans CJK SC, DejaVu Sans, Bitstream Vera Sans, Computer Modern Sans Serif, Lucida Grande, Verdana, Geneva, Lucid, Arial, Helvetica, Avant Garde, sans-serif"
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['mathtext.fontset'] = 'cm'

from rdkit import Chem
from rdkit.Chem import Descriptors, rdMolDescriptors
from rdkit.Chem.QED import qed
from sklearn.preprocessing import StandardScaler, MinMaxScaler, OneHotEncoder
from sklearn.impute import KNNImputer
from sklearn.decomposition import PCA
from sklearn.model_selection import train_test_split
import os


# ============================================================================
# 第一部分：数据生成 - 使用真实制冷剂分子
# ============================================================================

class RefrigerantDatabase:
    """
    制冷剂多尺度物理信息数据库
    
    数据来源：
    1. 真实制冷剂分子（来自 NIST、ASHRAE 标准）
    2. 量子化学数据（来自 QM9、PubChemQC 或 DFT 计算）
    3. 热力学数据（来自 NIST WebBook、REFPROP）
    4. 环境数据（来自 IPCC、EPA）
    """
    
    # 真实制冷剂分子数据库
    REAL_REFRIGERANTS = {
        # CFCs (第一代)
        'CFC': [
            {'name': 'R-11', 'smiles': 'C(F)(Cl)(Cl)Cl'},
            {'name': 'R-12', 'smiles': 'C(F)(F)(Cl)Cl'},
            {'name': 'R-113', 'smiles': 'FC(Cl)(Cl)C(F)(Cl)F'},
            {'name': 'R-114', 'smiles': 'FC(Cl)(F)C(Cl)(F)F'},
            {'name': 'R-115', 'smiles': 'C(F)(F)(F)C(F)(Cl)F'},
        ],
        # HCFCs (第二代)
        'HCFC': [
            {'name': 'R-21', 'smiles': 'C(F)(Cl)Cl'},
            {'name': 'R-22', 'smiles': 'C(F)(Cl)F'},
            {'name': 'R-123', 'smiles': 'C(F)(F)(Cl)C(Cl)Cl'},
            {'name': 'R-124', 'smiles': 'C(F)(F)(F)C(Cl)F'},
            {'name': 'R-141b', 'smiles': 'CC(F)(Cl)Cl'},
            {'name': 'R-142b', 'smiles': 'CC(F)(Cl)F'},
        ],
        # HFCs (第三代)
        'HFC': [
            {'name': 'R-32', 'smiles': 'C(F)F'},
            {'name': 'R-125', 'smiles': 'C(F)(F)(F)C(F)F'},
            {'name': 'R-134a', 'smiles': 'C(F)(F)(F)C(F)F'},
            {'name': 'R-143a', 'smiles': 'C(F)(F)(F)C(F)F'},
            {'name': 'R-152a', 'smiles': 'CC(F)F'},
            {'name': 'R-245fa', 'smiles': 'C(F)(F)(F)CC(F)F'},
            {'name': 'R-365mfc', 'smiles': 'C(F)(F)(F)CCC(F)F'},
        ],
        # HFOs (第四代)
        'HFO': [
            {'name': 'R-1234yf', 'smiles': 'C=C(C(F)(F)F)F'},
            {'name': 'R-1234ze(E)', 'smiles': 'FC=C(C)F'},
            {'name': 'R-1234ze(Z)', 'smiles': 'F\\C=C\\(C)F'},
            {'name': 'R-1336mzz(Z)', 'smiles': 'C(F)(F)(F)C=CC(F)(F)F'},
            {'name': 'R-1233zd(E)', 'smiles': 'FC=C(Cl)C(F)(F)F'},
        ],
        # 天然工质
        'Natural': [
            {'name': 'R-744 (CO2)', 'smiles': 'O=C=O'},
            {'name': 'R-717 (NH3)', 'smiles': 'N'},
            {'name': 'R-290 (Propane)', 'smiles': 'CCC'},
            {'name': 'R-600a (Isobutane)', 'smiles': 'CC(C)C'},
            {'name': 'R-600 (Butane)', 'smiles': 'CCCC'},
        ],
        # c-HFCs (环状)
        'c-HFC': [
            {'name': 'RC318', 'smiles': 'C1(F)C(F)(F)C(F)(F)C1(F)F'},
        ],
    }
    
    def __init__(self):
        self.df = None
        
    def generate_sample_data(self, n_samples_per_type=50):
        """
        生成多尺度物理信息数据集
        
        参数:
            n_samples_per_type: 每种类型的样本数（通过数据增强）
        """
        np.random.seed(42)
        
        data = []
        
        for ref_type, refrigerants in self.REAL_REFRIGERANTS.items():
            for ref in refrigerants:
                # 基础分子信息
                base = {
                    'id': f"{ref_type}_{ref['name'].replace(' ', '_')}",
                    'name': ref['name'],
                    'type': ref_type,
                    'smiles': ref['smiles'],
                    'cas_number': self._generate_cas()
                }
                
                # 量子化学数据
                quantum = self._generate_quantum_data(ref_type, ref['name'])
                
                # 热力学数据
                thermo = self._generate_thermo_data(ref_type, ref['name'])
                
                # 环境与安全数据
                env = self._generate_environmental_data(ref_type, ref['name'])
                
                data.append({**base, **quantum, **thermo, **env})
                
                # 数据增强：添加微小变异
                for i in range(min(n_samples_per_type // len(refrigerants), 10)):
                    variant = base.copy()
                    variant['id'] = f"{ref['name'].replace(' ', '_')}_var{i}"
                    variant['cas_number'] = self._generate_cas()
                    
                    # 添加小扰动
                    q_var = {k: v + np.random.normal(0, abs(v)*0.05) if isinstance(v, (int, float)) else v 
                            for k, v in quantum.items()}
                    t_var = {k: v + np.random.normal(0, abs(v)*0.05) if isinstance(v, (int, float)) else v 
                            for k, v in thermo.items()}
                    e_var = {k: v + np.random.normal(0, abs(v)*0.1) if isinstance(v, (int, float)) else v 
                            for k, v in env.items()}
                    
                    data.append({**variant, **q_var, **t_var, **e_var})
        
        self.df = pd.DataFrame(data)
        return self.df
    
    def _generate_cas(self):
        """生成随机 CAS 号"""
        return f"{np.random.randint(1000, 99999)}-{np.random.randint(10, 99)}-{np.random.randint(1, 9)}"
    
    def _generate_quantum_data(self, ref_type, name):
        """
        生成量子化学尺度数据
        
        实际应用中应从：
        - QM9 数据库
        - PubChemQC
        - 自主 DFT 计算 (B3LYP/6-311+G(d,p))
        """
        # 基于文献的典型值
        base_values = {
            'CFC': {'homo': -12.5, 'lumo': -1.5, 'gap': 11.0, 'dipole': 0.5},
            'HCFC': {'homo': -12.0, 'lumo': -1.2, 'gap': 10.8, 'dipole': 1.2},
            'HFC': {'homo': -13.0, 'lumo': -0.8, 'gap': 12.2, 'dipole': 2.0},
            'HFO': {'homo': -10.5, 'lumo': -0.5, 'gap': 10.0, 'dipole': 1.5},
            'Natural': {'homo': -11.0, 'lumo': 0.5, 'gap': 11.5, 'dipole': 0.3},
            'c-HFC': {'homo': -12.2, 'lumo': -0.9, 'gap': 11.3, 'dipole': 0.8},
        }
        
        base = base_values.get(ref_type, base_values['HFC'])
        
        return {
            'homo_ev': float(base['homo'] + np.random.normal(0, 0.3)),
            'lumo_ev': float(base['lumo'] + np.random.normal(0, 0.2)),
            'gap_ev': float(base['gap'] + np.random.normal(0, 0.3)),
            'dipole_debye': float(max(0.1, base['dipole'] + np.random.normal(0, 0.3))),
            'polarizability_au': float(np.random.uniform(25, 70)),
            'binding_energy_kjmol': float(np.random.uniform(-450, -250)),
            'electron_affinity_ev': float(np.random.uniform(-1.5, 0.5)),
            'ionization_potential_ev': float(np.random.uniform(10, 13))
        }
    
    def _generate_thermo_data(self, ref_type, name):
        """
        生成热力学尺度数据
        
        实际应用中应从：
        - NIST Chemistry WebBook
        - NIST REFPROP
        - ThermoML 数据库
        """
        # 基于真实制冷剂数据
        thermo_data = {
            'R-11': {'bp': 23.8, 'tc': 198, 'pc': 4.41},
            'R-12': {'bp': -29.8, 'tc': 112, 'pc': 4.14},
            'R-22': {'bp': -40.8, 'tc': 96, 'pc': 4.99},
            'R-32': {'bp': -51.7, 'tc': 78, 'pc': 5.78},
            'R-134a': {'bp': -26.3, 'tc': 101, 'pc': 4.06},
            'R-125': {'bp': -48.5, 'tc': 66, 'pc': 3.62},
            'R-1234yf': {'bp': -29.4, 'tc': 95, 'pc': 3.38},
            'R-744': {'bp': -78.5, 'tc': 31, 'pc': 7.38},
            'R-717': {'bp': -33.3, 'tc': 132, 'pc': 11.33},
            'R-290': {'bp': -42.1, 'tc': 97, 'pc': 4.25},
            'R-600a': {'bp': -11.7, 'tc': 135, 'pc': 3.64},
        }
        
        base = thermo_data.get(name.split()[0], {'bp': -30, 'tc': 100, 'pc': 4.0})
        
        return {
            'boiling_point_c': float(base['bp'] + np.random.normal(0, 2)),
            'critical_temp_c': float(base['tc'] + np.random.normal(0, 5)),
            'critical_pressure_mpa': float(base['pc'] + np.random.normal(0, 0.2)),
            'critical_density_kgm3': float(np.random.uniform(400, 600)),
            'vapor_pressure_kpa': float(np.random.uniform(100, 700)),
            'liquid_density_kgm3': float(np.random.uniform(1000, 1500)),
            'viscosity_upas': float(np.random.uniform(8, 18)),
            'thermal_conductivity_wmk': float(np.random.uniform(0.06, 0.14)),
            'heat_capacity_jmolk': float(np.random.uniform(80, 140)),
            'vaporization_enthalpy_kjmol': float(np.random.uniform(18, 30)),
            'cop': float(np.random.uniform(2.8, 5.2)),
            'volumetric_cooling_mjm3': float(np.random.uniform(2.5, 7.5))
        }
    
    def _generate_environmental_data(self, ref_type, name):
        """
        生成环境与安全数据
        
        实际应用中应从：
        - IPCC Assessment Reports (GWP)
        - EPA databases
        - ASHRAE Standard 34
        """
        # 基于真实数据
        env_data = {
            'CFC': {'gwp': 5000, 'odp': 0.9, 'lifetime': 50, 'safety': 'A1'},
            'HCFC': {'gwp': 500, 'odp': 0.05, 'lifetime': 15, 'safety': 'A1'},
            'HFC': {'gwp': 1500, 'odp': 0, 'lifetime': 15, 'safety': 'A1'},
            'HFO': {'gwp': 4, 'odp': 0, 'lifetime': 0.03, 'safety': 'A2L'},
            'Natural': {'gwp': 3, 'odp': 0, 'lifetime': 0.5, 'safety': 'A3'},
            'c-HFC': {'gwp': 10000, 'odp': 0, 'lifetime': 3200, 'safety': 'A1'},
        }
        
        base = env_data.get(ref_type, env_data['HFC'])
        
        flammability = base['safety']
        if ref_type == 'HFO':
            flammability = np.random.choice(['A2L', 'A2'], p=[0.7, 0.3])
        elif ref_type == 'Natural' and name not in ['R-744', 'R-717']:
            flammability = 'A3'
        
        return {
            'gwp_100yr': float(base['gwp'] * np.random.uniform(0.8, 1.2)),
            'gwp_20yr': float(base['gwp'] * np.random.uniform(1.2, 1.8)),
            'odp': float(base['odp'] * np.random.uniform(0.8, 1.2)),
            'atmospheric_lifetime_yr': float(base['lifetime'] * np.random.uniform(0.7, 1.3)),
            'flammability_class': flammability,
            'toxicity_class': 'A' if np.random.random() > 0.15 else 'B',
            'ashrae_safety': base['safety'] if np.random.random() > 0.2 else flammability,
            'tfa_yield_percent': float(np.random.uniform(10, 60) if ref_type in ['HFO', 'HFC'] else 0),
            'synthetic_accessibility': float(np.random.uniform(2.5, 7.5))
        }


# ============================================================================
# 第二部分：数据预处理
# ============================================================================

class DataPreprocessor:
    """数据预处理器"""
    
    def __init__(self):
        self.scalers = {}
        self.encoders = {}
        self.imputers = {}
        self.feature_columns = []
        
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """数据清洗"""
        df_clean = df.copy()
        
        # 去重
        initial = len(df_clean)
        df_clean = df_clean.drop_duplicates(subset=['smiles'], keep='first')
        print(f"去除重复：{initial - len(df_clean)} 个分子")
        
        # 异常值处理 (IQR)
        numeric_cols = df_clean.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            Q1 = df_clean[col].quantile(0.25)
            Q3 = df_clean[col].quantile(0.75)
            IQR = Q3 - Q1
            df_clean[col] = df_clean[col].clip(Q1 - 3*IQR, Q3 + 3*IQR)
        
        return df_clean
    
    def engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """特征工程"""
        df_feat = df.copy()
        
        # RDKit 描述符
        print("计算 RDKit 分子描述符...")
        rdkit_features = []
        
        for _, row in df_feat.iterrows():
            try:
                mol = Chem.MolFromSmiles(row['smiles'])
                if mol:
                    feat = {
                        'mol_weight': float(Descriptors.MolWt(mol)),
                        'logp': float(Descriptors.MolLogP(mol)),
                        'tpsa': float(Descriptors.TPSA(mol)),
                        'num_atoms': float(mol.GetNumAtoms()),
                        'num_rings': float(rdMolDescriptors.CalcNumRings(mol)),
                        'qed_score': float(qed(mol))
                    }
                else:
                    feat = {k: np.nan for k in ['mol_weight', 'logp', 'tpsa', 'num_atoms', 'num_rings', 'qed_score']}
            except:
                feat = {k: np.nan for k in ['mol_weight', 'logp', 'tpsa', 'num_atoms', 'num_rings', 'qed_score']}
            rdkit_features.append(feat)
        
        df_feat = pd.concat([df_feat, pd.DataFrame(rdkit_features)], axis=1)
        
        # 衍生特征
        df_feat['efficiency_index'] = df_feat['cop'] / (df_feat['gwp_100yr'] + 1)
        df_feat['environmental_risk'] = (df_feat['gwp_100yr']/1000 + df_feat['odp']*100 + 
                                          df_feat['atmospheric_lifetime_yr']/50)
        
        safety_map = {'A1': 1, 'A2L': 2, 'A2': 3, 'A3': 4, 'B1': 5, 'B2': 6, 'B3': 7}
        df_feat['safety_score'] = df_feat['ashrae_safety'].map(safety_map)
        
        df_feat['comprehensive_score'] = (df_feat['cop']/5.0 - df_feat['gwp_100yr']/5000 - 
                                           df_feat['safety_score']/7.0)
        
        # 量子化学衍生
        df_feat['electrophilicity'] = (df_feat['homo_ev'] + df_feat['lumo_ev'])**2 / (8 * df_feat['gap_ev'].clip(0.1, None))
        df_feat['chemical_hardness'] = df_feat['gap_ev'] / 2
        df_feat['electronegativity'] = -(df_feat['homo_ev'] + df_feat['lumo_ev']) / 2
        
        print(f"新增特征：{len(rdkit_features[0]) + 6} 个")
        return df_feat
    
    def handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """缺失值处理"""
        df_imp = df.copy()
        
        # 删除高缺失率列
        missing = df_imp.isnull().sum() / len(df_imp)
        cols_drop = missing[missing > 0.2].index.tolist()
        if cols_drop:
            print(f"删除高缺失率列：{cols_drop}")
            df_imp = df_imp.drop(columns=cols_drop)
        
        # KNN 插补
        num_cols = df_imp.select_dtypes(include=[np.number]).columns
        if len(num_cols) > 0:
            self.imputers['numeric'] = KNNImputer(n_neighbors=5)
            df_imp[num_cols] = self.imputers['numeric'].fit_transform(df_imp[num_cols])
        
        # 众数填充
        cat_cols = df_imp.select_dtypes(include=['object']).columns
        for col in cat_cols:
            if df_imp[col].isnull().any():
                df_imp[col] = df_imp[col].fillna(df_imp[col].mode()[0])
        
        return df_imp
    
    def normalize_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """特征标准化"""
        df_norm = df.copy()
        
        # 数值列
        exclude = ['id', 'name', 'cas_number']
        num_cols = [c for c in df_norm.select_dtypes(include=[np.number]).columns if c not in exclude]
        self.feature_columns = num_cols.copy()
        
        # MinMax 物理量
        phys_cols = ['boiling_point_c', 'critical_temp_c', 'critical_pressure_mpa', 'cop']
        phys_cols = [c for c in phys_cols if c in num_cols]
        if phys_cols:
            self.scalers['physical'] = MinMaxScaler()
            df_norm[phys_cols] = self.scalers['physical'].fit_transform(df_norm[phys_cols])
        
        # Z-score 能量
        energy_cols = ['homo_ev', 'lumo_ev', 'gap_ev', 'binding_energy_kjmol']
        energy_cols = [c for c in energy_cols if c in num_cols]
        if energy_cols:
            self.scalers['energy'] = StandardScaler()
            df_norm[energy_cols] = self.scalers['energy'].fit_transform(df_norm[energy_cols])
        
        # 对数变换
        env_cols = ['gwp_100yr', 'atmospheric_lifetime_yr']
        for col in env_cols:
            if col in df_norm.columns:
                df_norm[f'{col}_log'] = np.log1p(df_norm[col])
        
        # One-hot 编码
        cat_cols = ['type', 'flammability_class', 'ashrae_safety']
        cat_cols = [c for c in cat_cols if c in df_norm.columns]
        if cat_cols:
            self.encoders['cat'] = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
            encoded = self.encoders['cat'].fit_transform(df_norm[cat_cols])
            enc_df = pd.DataFrame(encoded, 
                                 columns=self.encoders['cat'].get_feature_names_out(cat_cols),
                                 index=df_norm.index)
            df_norm = pd.concat([df_norm, enc_df], axis=1)
            df_norm = df_norm.drop(columns=cat_cols)
        
        return df_norm
    
    def create_splits(self, df: pd.DataFrame) -> Dict:
        """数据集划分"""
        train, temp = train_test_split(df, test_size=0.3, random_state=42)
        val, test = train_test_split(temp, test_size=0.5, random_state=42)
        
        print(f"数据集：训练集 {len(train)}, 验证集 {len(val)}, 测试集 {len(test)}")
        return {'train': train, 'val': val, 'test': test}


# ============================================================================
# 第三部分：数据分析
# ============================================================================

class DataAnalyzer:
    """数据分析器"""
    
    def __init__(self, save_dir='data_analysis'):
        os.makedirs(save_dir, exist_ok=True)
        self.save_dir = save_dir
    
    def plot_distributions(self, df: pd.DataFrame):
        """特征分布"""
        fig, axes = plt.subplots(3, 3, figsize=(15, 12))
        axes = axes.flatten()
        
        features = [
            ('gwp_100yr', 'GWP (100 年)'),
            ('cop', 'COP'),
            ('boiling_point_c', '沸点 (°C)'),
            ('homo_ev', 'HOMO (eV)'),
            ('gap_ev', '能隙 (eV)'),
            ('mol_weight', '分子量'),
            ('logp', 'LogP'),
            ('efficiency_index', '效率指数'),
            ('comprehensive_score', '综合评分')
        ]
        
        for idx, (col, label) in enumerate(features):
            if col in df.columns:
                data = df[col].dropna()
                if len(data) > 0:
                    axes[idx].hist(data, bins=25, edgecolor='black', alpha=0.7, color='steelblue')
                    axes[idx].set_xlabel(label)
                    axes[idx].set_ylabel('频数')
                    axes[idx].set_title(f'{label} 分布')
                    axes[idx].grid(True, alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(f'{self.save_dir}/distributions.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"分布图：{self.save_dir}/distributions.png")
    
    def plot_correlations(self, df: pd.DataFrame):
        """相关性热图"""
        num_df = df.select_dtypes(include=[np.number])
        
        features = ['gwp_100yr', 'cop', 'homo_ev', 'gap_ev', 'mol_weight', 
                   'logp', 'efficiency_index', 'comprehensive_score']
        features = [f for f in features if f in num_df.columns]
        
        if len(features) >= 2:
            corr = num_df[features].corr()
            
            fig, ax = plt.subplots(figsize=(10, 8))
            im = ax.imshow(corr, cmap='RdBu_r', vmin=-1, vmax=1)
            plt.colorbar(im, ax=ax, label='相关系数')
            
            ax.set_xticks(range(len(features)))
            ax.set_yticks(range(len(features)))
            ax.set_xticklabels(features, rotation=45, ha='right')
            ax.set_yticklabels(features)
            
            for i in range(len(features)):
                for j in range(len(features)):
                    ax.text(j, i, f'{corr.iloc[i,j]:.2f}', ha='center', va='center', fontsize=8)
            
            ax.set_title('特征相关性热图')
            plt.tight_layout()
            plt.savefig(f'{self.save_dir}/correlations.png', dpi=300, bbox_inches='tight')
            plt.close()
            print(f"相关图：{self.save_dir}/correlations.png")
    
    def perform_pca(self, df: pd.DataFrame):
        """PCA 分析"""
        num_df = df.select_dtypes(include=[np.number]).dropna(axis=1, how='all').dropna()
        
        if len(num_df) < 10 or num_df.shape[1] < 2:
            print("样本不足，跳过 PCA")
            return
        
        scaler = StandardScaler()
        scaled = scaler.fit_transform(num_df)
        
        pca = PCA(n_components=min(2, scaled.shape[1]-1))
        result = pca.fit_transform(scaled)
        
        plt.figure(figsize=(8, 6))
        plt.scatter(result[:, 0], result[:, 1], alpha=0.6, c='steelblue')
        plt.xlabel(f'PC1 ({pca.explained_variance_ratio_[0]:.1%})')
        plt.ylabel(f'PC2 ({pca.explained_variance_ratio_[1]:.1%})')
        plt.title('化学空间 PCA')
        plt.tight_layout()
        plt.savefig(f'{self.save_dir}/pca.png', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"PCA 图：{self.save_dir}/pca.png")
    
    def generate_report(self, df: pd.DataFrame) -> Dict:
        """生成报告"""
        report = {
            'total_molecules': len(df),
            'total_features': len(df.columns),
            'type_dist': df['type'].value_counts().to_dict() if 'type' in df.columns else {},
            'stats': {}
        }
        
        for col in df.select_dtypes(include=[np.number]).columns[:15]:
            if df[col].notna().any():
                report['stats'][col] = {
                    'mean': float(df[col].mean()),
                    'std': float(df[col].std()),
                    'min': float(df[col].min()),
                    'max': float(df[col].max())
                }
        
        with open(f'{self.save_dir}/report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        return report


# ============================================================================
# 主流程
# ============================================================================

def main():
    print("=" * 80)
    print("多尺度物理信息嵌入的制冷剂分子设计")
    print("数据收集与预处理实验流程")
    print("=" * 80)
    
    # 1. 数据收集
    print("\n【步骤 1】数据收集...")
    db = RefrigerantDatabase()
    df = db.generate_sample_data(n_samples_per_type=30)
    print(f"初始数据：{len(df)} 个分子，{len(df.columns)} 个特征")
    
    # 2. 预处理
    print("\n【步骤 2】数据预处理...")
    prep = DataPreprocessor()
    
    print("  2.1 清洗...")
    df_clean = prep.clean_data(df)
    
    print("  2.2 特征工程...")
    df_feat = prep.engineer_features(df_clean)
    
    print("  2.3 缺失值处理...")
    df_imp = prep.handle_missing_values(df_feat)
    
    print("  2.4 标准化...")
    df_norm = prep.normalize_features(df_imp)
    
    print("  2.5 数据集划分...")
    splits = prep.create_splits(df_norm)
    
    # 3. 分析
    print("\n【步骤 3】数据分析...")
    analyzer = DataAnalyzer()
    
    analyzer.plot_distributions(df_norm)
    analyzer.plot_correlations(df_norm)
    analyzer.perform_pca(df_norm)
    report = analyzer.generate_report(df_norm)
    
    # 4. 保存
    print("\n【步骤 4】保存数据...")
    df_norm.to_csv('refrigerant_processed.csv', index=False, encoding='utf-8-sig')
    for name, data in splits.items():
        data.to_csv(f'{name}_set.csv', index=False, encoding='utf-8-sig')
    
    # 5. 总结
    print("\n" + "=" * 80)
    print("流程完成！")
    print("=" * 80)
    print(f"\n输出:")
    print(f"  - refrigerant_processed.csv")
    print(f"  - train_set.csv ({len(splits['train'])} 样本)")
    print(f"  - validation_set.csv ({len(splits['val'])} 样本)")
    print(f"  - test_set.csv ({len(splits['test'])} 样本)")
    print(f"  - data_analysis/")
    print(f"\n统计:")
    print(f"  - 分子数：{report['total_molecules']}")
    print(f"  - 特征数：{report['total_features']}")
    print(f"\n类型分布:")
    for t, c in report.get('type_dist', {}).items():
        print(f"  - {t}: {c}")
    
    return df_norm, splits, report


if __name__ == "__main__":
    main()
