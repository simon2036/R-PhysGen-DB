"""Shared constants for R-PhysGen-DB V2."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
SCHEMA_DIR = PROJECT_ROOT / "schemas"
DOCS_DIR = PROJECT_ROOT / "docs"

PARSER_VERSION = "r-physgen-db-v2"

SOURCE_PRIORITY = {
    "manual_curated_reference": 100,
    "public_database": 95,
    "public_web_snapshot": 90,
    "calculated_open_source": 80,
    "derived_harmonized": 70,
    "manual_catalog": 40,
    "placeholder": 10,
}

QUALITY_SCORES = {
    "manual_curated_reference": 1.00,
    "primary_public_reference": 0.95,
    "derived_harmonized": 0.85,
    "calculated_open_source": 0.80,
    "snapshot_only": 0.60,
    "placeholder": 0.20,
}

NUMERIC_PROPERTIES = {
    "boiling_point_c",
    "critical_temp_c",
    "critical_pressure_mpa",
    "critical_density_kgm3",
    "acentric_factor",
    "vaporization_enthalpy_kjmol",
    "cop_standard_cycle",
    "volumetric_cooling_mjm3",
    "gwp_20yr",
    "gwp_100yr",
    "gwp_ar4_100yr",
    "gwp_ar5_100yr",
    "gwp_ar6_100yr",
    "odp",
    "atmospheric_lifetime_yr",
}

CATEGORICAL_PROPERTIES = {
    "ashrae_safety",
    "toxicity_class",
}

MODEL_TARGET_PROPERTIES = [
    "boiling_point_c",
    "critical_temp_c",
    "critical_pressure_mpa",
    "vaporization_enthalpy_kjmol",
    "cop_standard_cycle",
    "volumetric_cooling_mjm3",
    "gwp_100yr",
    "odp",
    "ashrae_safety",
    "toxicity_class",
]

GWP_PREFERENCE_ORDER = [
    "gwp_ar6_100yr",
    "gwp_ar5_100yr",
    "gwp_ar4_100yr",
    "gwp_100yr",
]

STANDARD_CYCLE = {
    "evaporating_temp_c": 5.0,
    "condensing_temp_c": 50.0,
    "superheat_k": 5.0,
    "subcooling_k": 5.0,
    "compressor_isentropic_efficiency": 0.75,
}

TRANSCRITICAL_CO2_CYCLE = {
    "evaporating_temp_c": -5.0,
    "gas_cooler_outlet_temp_c": 35.0,
    "high_side_pressure_mpa": 9.0,
    "superheat_k": 5.0,
    "compressor_isentropic_efficiency": 0.70,
}

SNAP_SOURCE_PAGES = [
    {
        "key": "cold_storage_warehouses",
        "end_use": "cold_storage_warehouses",
        "url": "https://www.epa.gov/snap/substitutes-cold-storage-warehouses",
    },
    {
        "key": "commercial_ice_machines",
        "end_use": "commercial_ice_machines",
        "url": "https://www.epa.gov/snap/substitutes-commercial-ice-machines",
    },
    {
        "key": "household_refrigerators_and_freezers",
        "end_use": "household_refrigerators_and_freezers",
        "url": "https://www.epa.gov/snap/substitutes-household-refrigerators-and-freezers",
    },
    {
        "key": "ice_skating_rinks",
        "end_use": "ice_skating_rinks",
        "url": "https://www.epa.gov/snap/substitutes-ice-skating-rinks",
    },
    {
        "key": "industrial_process_air_conditioning",
        "end_use": "industrial_process_air_conditioning",
        "url": "https://www.epa.gov/snap/substitutes-industrial-process-air-conditioning",
    },
    {
        "key": "industrial_process_refrigeration",
        "end_use": "industrial_process_refrigeration",
        "url": "https://www.epa.gov/snap/substitutes-industrial-process-refrigeration",
    },
    {
        "key": "non_mechanical_heat_transfer_systems",
        "end_use": "non_mechanical_heat_transfer_systems",
        "url": "https://www.epa.gov/snap/substitutes-non-mechanical-heat-transfer-systems",
    },
    {
        "key": "residential_and_light_commercial_air_conditioning_and_heat_pumps",
        "end_use": "residential_and_light_commercial_air_conditioning_and_heat_pumps",
        "url": "https://www.epa.gov/snap/substitutes-residential-and-light-commercial-air-conditioning-and-heat-pumps",
    },
    {
        "key": "residential_dehumidifiers",
        "end_use": "residential_dehumidifiers",
        "url": "https://www.epa.gov/snap/substitutes-residential-dehumidifiers",
    },
    {
        "key": "refrigerated_transport",
        "end_use": "refrigerated_transport",
        "url": "https://www.epa.gov/snap/substitutes-refrigerated-transport",
    },
    {
        "key": "refrigerated_food_processing_and_dispensing_equipment",
        "end_use": "refrigerated_food_processing_and_dispensing_equipment",
        "url": "https://www.epa.gov/snap/substitutes-refrigerated-food-processing-and-dispensing-equipment",
    },
    {
        "key": "remote_condensing_units",
        "end_use": "remote_condensing_units",
        "url": "https://www.epa.gov/snap/substitutes-remote-condensing-units",
    },
    {
        "key": "typical_supermarket_systems",
        "end_use": "typical_supermarket_systems",
        "url": "https://www.epa.gov/snap/substitutes-typical-supermarket-systems",
    },
    {
        "key": "vending_machines",
        "end_use": "vending_machines",
        "url": "https://www.epa.gov/snap/substitutes-vending-machines",
    },
    {
        "key": "very_low_temperature_refrigeration",
        "end_use": "very_low_temperature_refrigeration",
        "url": "https://www.epa.gov/snap/substitutes-very-low-temperature-refrigeration",
    },
    {
        "key": "water_coolers",
        "end_use": "water_coolers",
        "url": "https://www.epa.gov/snap/substitutes-water-coolers",
    },
    {
        "key": "centrifugal_chillers",
        "end_use": "centrifugal_chillers",
        "url": "https://www.epa.gov/snap/substitutes-centrifugal-chillers",
    },
    {
        "key": "positive_displacement_chillers",
        "end_use": "positive_displacement_chillers",
        "url": "https://www.epa.gov/snap/substitutes-positive-displacement-chillers",
    },
]

EPA_TECHNOLOGY_TRANSITIONS_GWP_URL = "https://www.epa.gov/climate-hfcs-reduction/technology-transitions-gwp-reference-table"

COMPTOX_ENV_VAR_NAMES = [
    "COMPT0X_API_KEY",
    "COMPTOX_API_KEY",
    "EPA_COMPTOX_API_KEY",
]

DUCKDB_TABLES = [
    "source_manifest",
    "pending_sources",
    "seed_resolution",
    "molecule_core",
    "molecule_alias",
    "property_observation",
    "regulatory_status",
    "property_recommended",
    "structure_features",
    "molecule_master",
    "property_matrix",
    "model_dataset_index",
    "model_ready",
]
