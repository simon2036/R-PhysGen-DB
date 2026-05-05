"""CoolProp-derived thermodynamic and cycle labels."""

from __future__ import annotations

from typing import Any

import CoolProp
from CoolProp.CoolProp import FluidsList, PropsSI

from r_physgen_db.constants import STANDARD_CYCLE, TRANSCRITICAL_CO2_CYCLE
from r_physgen_db.cycle_conditions import operating_point_hash


SUBCRITICAL_CYCLE_POINTS = [
    {
        "cycle_case_id": "subcritical_5c_35c_cycle",
        "case_name": "5 degC evaporating / 35 degC condensing",
        "evaporating_temperature_c": 5.0,
        "condensing_temperature_c": 35.0,
        "superheat_k": STANDARD_CYCLE["superheat_k"],
        "subcooling_k": STANDARD_CYCLE["subcooling_k"],
        "compressor_isentropic_efficiency": STANDARD_CYCLE["compressor_isentropic_efficiency"],
    },
    {
        "cycle_case_id": "standard_subcritical_cycle",
        "case_name": "5 degC evaporating / 50 degC condensing",
        "evaporating_temperature_c": STANDARD_CYCLE["evaporating_temp_c"],
        "condensing_temperature_c": STANDARD_CYCLE["condensing_temp_c"],
        "superheat_k": STANDARD_CYCLE["superheat_k"],
        "subcooling_k": STANDARD_CYCLE["subcooling_k"],
        "compressor_isentropic_efficiency": STANDARD_CYCLE["compressor_isentropic_efficiency"],
    },
    {
        "cycle_case_id": "subcritical_minus10c_40c_cycle",
        "case_name": "-10 degC evaporating / 40 degC condensing",
        "evaporating_temperature_c": -10.0,
        "condensing_temperature_c": 40.0,
        "superheat_k": STANDARD_CYCLE["superheat_k"],
        "subcooling_k": STANDARD_CYCLE["subcooling_k"],
        "compressor_isentropic_efficiency": STANDARD_CYCLE["compressor_isentropic_efficiency"],
    },
]

TRANSCRITICAL_CO2_CYCLE_POINTS = [
    {
        "cycle_case_id": "transcritical_co2_cycle",
        "case_name": "-5 degC evaporating / 35 degC gas cooler / 9 MPa high side",
        "evaporating_temperature_c": TRANSCRITICAL_CO2_CYCLE["evaporating_temp_c"],
        "gas_cooler_outlet_temperature_c": TRANSCRITICAL_CO2_CYCLE["gas_cooler_outlet_temp_c"],
        "high_side_pressure_mpa": TRANSCRITICAL_CO2_CYCLE["high_side_pressure_mpa"],
        "superheat_k": TRANSCRITICAL_CO2_CYCLE["superheat_k"],
        "compressor_isentropic_efficiency": TRANSCRITICAL_CO2_CYCLE["compressor_isentropic_efficiency"],
    },
    {
        "cycle_case_id": "transcritical_co2_low_temp_cycle",
        "case_name": "-10 degC evaporating / 35 degC gas cooler / 9 MPa high side",
        "evaporating_temperature_c": -10.0,
        "gas_cooler_outlet_temperature_c": 35.0,
        "high_side_pressure_mpa": 9.0,
        "superheat_k": TRANSCRITICAL_CO2_CYCLE["superheat_k"],
        "compressor_isentropic_efficiency": TRANSCRITICAL_CO2_CYCLE["compressor_isentropic_efficiency"],
    },
    {
        "cycle_case_id": "transcritical_co2_high_lift_cycle",
        "case_name": "0 degC evaporating / 40 degC gas cooler / 10 MPa high side",
        "evaporating_temperature_c": 0.0,
        "gas_cooler_outlet_temperature_c": 40.0,
        "high_side_pressure_mpa": 10.0,
        "superheat_k": TRANSCRITICAL_CO2_CYCLE["superheat_k"],
        "compressor_isentropic_efficiency": TRANSCRITICAL_CO2_CYCLE["compressor_isentropic_efficiency"],
    },
]


class UnsupportedCoolPropFluidError(ValueError):
    """Raised when a fluid is intentionally left unresolved for CoolProp calculations."""


class CoolPropSource:
    def __init__(self) -> None:
        self.version = getattr(CoolProp, "__version__", "unknown")
        self.available_fluids = set(FluidsList())

    def session_metadata(self) -> dict[str, Any]:
        return {
            "coolprop_version": self.version,
            "standard_cycle": STANDARD_CYCLE,
            "transcritical_co2_cycle": TRANSCRITICAL_CO2_CYCLE,
            "production_cycle_points": {
                "subcritical": SUBCRITICAL_CYCLE_POINTS,
                "transcritical_co2": TRANSCRITICAL_CO2_CYCLE_POINTS,
            },
            "transport_observations": {
                "basis": "saturated_liquid_at_101325_pa",
                "properties": ["viscosity_liquid_pas", "thermal_conductivity_liquid_wmk"],
            },
            "available_fluid_count": len(self.available_fluids),
        }

    def supports(self, fluid: str) -> bool:
        return bool(fluid) and fluid in self.available_fluids

    def generate_observations(self, mol_id: str, fluid: str, source_id: str) -> list[dict[str, Any]]:
        if not fluid:
            raise UnsupportedCoolPropFluidError("No explicit CoolProp fluid mapping provided.")
        if not self.supports(fluid):
            raise UnsupportedCoolPropFluidError(f"CoolProp fluid '{fluid}' is unsupported.")

        observations: list[dict[str, Any]] = []
        observations.extend(self._thermo_observations(mol_id, fluid, source_id))
        observations.extend(self._transport_observations(mol_id, fluid, source_id))

        for cycle_spec in self._cycle_specs(fluid):
            try:
                cycle = self._cycle_metrics(fluid, cycle_spec)
            except Exception as exc:  # noqa: BLE001 - keep resolved thermo labels when only one cycle path fails
                observations.extend(
                    self._resolution_observations(
                        mol_id,
                        source_id,
                        fluid,
                        status=f"cycle_unresolved:{type(exc).__name__}:{exc}",
                        cycle_spec=cycle_spec,
                    )
                )
                continue

            if cycle is None:
                observations.extend(self._resolution_observations(mol_id, source_id, fluid, status="unsupported", cycle_spec=cycle_spec))
                continue

            cycle_label = cycle["cycle_label"]
            for property_name, value_num, unit in [
                ("cop_standard_cycle", cycle["cop"], "dimensionless"),
                ("volumetric_cooling_mjm3", cycle["qvol"], "MJ/m3"),
                ("pressure_ratio", cycle["pressure_ratio"], "dimensionless"),
                ("discharge_temperature_c", cycle["discharge_temperature_c"], "degC"),
            ]:
                observations.append(
                    self._observation(
                        mol_id=mol_id,
                        property_name=property_name,
                        value_num=value_num,
                        unit=unit,
                        source_id=source_id,
                        temperature=cycle_label,
                        phase="cycle",
                        method=cycle["method"],
                        notes=cycle["status"],
                        cycle_case_id=cycle["cycle_case_id"],
                        operating_point_hash=cycle["operating_point_hash"],
                        operating_point_json=cycle["operating_point_json"],
                        cycle_model=cycle["cycle_model"],
                        eos_source=cycle["eos_source"],
                        convergence_flag=1,
                    )
                )
        return observations

    def _thermo_observations(self, mol_id: str, fluid: str, source_id: str) -> list[dict[str, Any]]:
        return [
            self._observation(
                mol_id=mol_id,
                property_name="boiling_point_c",
                value_num=PropsSI("T", "P", 101325, "Q", 0, fluid) - 273.15,
                unit="degC",
                source_id=source_id,
                phase="saturated_liquid",
                pressure="0.101325 MPa",
                method="CoolProp.PropsSI(T|P=101325 Pa,Q=0)",
            ),
            self._observation(
                mol_id=mol_id,
                property_name="critical_temp_c",
                value_num=PropsSI("Tcrit", fluid) - 273.15,
                unit="degC",
                source_id=source_id,
                method="CoolProp.PropsSI(Tcrit)",
            ),
            self._observation(
                mol_id=mol_id,
                property_name="critical_pressure_mpa",
                value_num=PropsSI("pcrit", fluid) / 1e6,
                unit="MPa",
                source_id=source_id,
                method="CoolProp.PropsSI(pcrit)",
            ),
            self._observation(
                mol_id=mol_id,
                property_name="critical_density_kgm3",
                value_num=PropsSI("rhomass_critical", fluid),
                unit="kg/m3",
                source_id=source_id,
                method="CoolProp.PropsSI(rhomass_critical)",
            ),
            self._observation(
                mol_id=mol_id,
                property_name="acentric_factor",
                value_num=PropsSI("acentric", fluid),
                unit="dimensionless",
                source_id=source_id,
                method="CoolProp.PropsSI(acentric)",
            ),
            self._observation(
                mol_id=mol_id,
                property_name="vaporization_enthalpy_kjmol",
                value_num=(
                    PropsSI("HMOLAR", "P", 101325, "Q", 1, fluid) - PropsSI("HMOLAR", "P", 101325, "Q", 0, fluid)
                )
                / 1000.0,
                unit="kJ/mol",
                source_id=source_id,
                phase="vapor-liquid_equilibrium",
                pressure="0.101325 MPa",
                method="CoolProp.PropsSI(HMOLAR@Q=1 - HMOLAR@Q=0,P=101325 Pa)",
            ),
        ]

    def _transport_observations(self, mol_id: str, fluid: str, source_id: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for output_key, property_name, unit in [
            ("V", "viscosity_liquid_pas", "Pa*s"),
            ("L", "thermal_conductivity_liquid_wmk", "W/(m*K)"),
        ]:
            try:
                value_num = PropsSI(output_key, "P", 101325, "Q", 0, fluid)
                rows.append(
                    self._observation(
                        mol_id=mol_id,
                        property_name=property_name,
                        value_num=value_num,
                        unit=unit,
                        source_id=source_id,
                        phase="saturated_liquid",
                        pressure="0.101325 MPa",
                        method=f"CoolProp.PropsSI({output_key}|P=101325 Pa,Q=0)",
                        notes="resolved:saturated_liquid_transport",
                    )
                )
            except Exception as exc:  # noqa: BLE001 - transport support is backend-dependent
                rows.append(
                    self._warning_observation(
                        mol_id=mol_id,
                        property_name=property_name,
                        unit=unit,
                        source_id=source_id,
                        method=f"CoolProp.PropsSI({output_key}|P=101325 Pa,Q=0)",
                        notes=f"transport_unresolved:{fluid}:{type(exc).__name__}:{exc}",
                    )
                )
        return rows

    def _warning_observation(
        self,
        *,
        mol_id: str,
        property_name: str,
        unit: str,
        source_id: str,
        method: str,
        notes: str,
    ) -> dict[str, Any]:
        return {
            "observation_id": None,
            "mol_id": mol_id,
            "property_name": property_name,
            "value": "",
            "value_num": None,
            "unit": unit,
            "temperature": "",
            "pressure": "0.101325 MPa",
            "phase": "saturated_liquid",
            "source_type": "calculated_open_source",
            "source_name": f"CoolProp {self.version}",
            "source_id": source_id,
            "method": method,
            "uncertainty": "",
            "quality_level": "calculated_open_source",
            "assessment_version": "",
            "time_horizon": "",
            "year": "",
            "notes": notes,
            "qc_status": "warning",
            "qc_flags": "transport_unresolved",
        }

    def _resolution_observations(
        self,
        mol_id: str,
        source_id: str,
        fluid: str,
        *,
        status: str,
        cycle_spec: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        cycle_context = self._cycle_context(cycle_spec or {}, cycle_model="transcritical_co2" if fluid == "CarbonDioxide" else "subcritical_vapor_compression")
        rows = []
        for property_name, unit in [
            ("cop_standard_cycle", "dimensionless"),
            ("volumetric_cooling_mjm3", "MJ/m3"),
            ("pressure_ratio", "dimensionless"),
            ("discharge_temperature_c", "degC"),
        ]:
            rows.append(
                {
                    "observation_id": None,
                    "mol_id": mol_id,
                    "property_name": property_name,
                    "value": "",
                    "value_num": None,
                    "unit": unit,
                    "temperature": cycle_spec.get("case_name", "") if cycle_spec else "",
                    "pressure": "",
                    "phase": "cycle",
                    "source_type": "calculated_open_source",
                    "source_name": f"CoolProp {self.version}",
                    "source_id": source_id,
                    "method": "CoolProp cycle path unresolved",
                    "uncertainty": "",
                    "quality_level": "calculated_open_source",
                    "assessment_version": "",
                    "time_horizon": "",
                    "year": "",
                    "notes": f"{status}:{fluid}",
                    "qc_status": "warning",
                    "qc_flags": "cycle_unresolved",
                    "cycle_case_id": cycle_context["cycle_case_id"],
                    "operating_point_hash": cycle_context["operating_point_hash"],
                    "operating_point_json": cycle_context["operating_point_json"],
                    "cycle_model": cycle_context["cycle_model"],
                    "eos_source": "CoolProp",
                    "convergence_flag": 0,
                }
            )
        return rows

    def _observation(
        self,
        *,
        mol_id: str,
        property_name: str,
        value_num: float,
        unit: str,
        source_id: str,
        temperature: str | None = None,
        pressure: str | None = None,
        phase: str | None = None,
        method: str,
        notes: str = "",
        cycle_case_id: str = "",
        operating_point_hash: str = "",
        operating_point_json: str = "",
        cycle_model: str = "",
        eos_source: str = "",
        convergence_flag: int | None = None,
    ) -> dict[str, Any]:
        row = {
            "observation_id": None,
            "mol_id": mol_id,
            "property_name": property_name,
            "value": f"{value_num:.8g}",
            "value_num": float(value_num),
            "unit": unit,
            "temperature": temperature or "",
            "pressure": pressure or "",
            "phase": phase or "",
            "source_type": "calculated_open_source",
            "source_name": f"CoolProp {self.version}",
            "source_id": source_id,
            "method": method,
            "uncertainty": "",
            "quality_level": "calculated_open_source",
            "assessment_version": "",
            "time_horizon": "",
            "year": "",
            "notes": notes,
            "qc_status": "pass",
            "qc_flags": "",
        }
        if cycle_case_id or operating_point_hash or cycle_model:
            row.update(
                {
                    "cycle_case_id": cycle_case_id,
                    "operating_point_hash": operating_point_hash,
                    "operating_point_json": operating_point_json,
                    "cycle_model": cycle_model,
                    "eos_source": eos_source,
                    "convergence_flag": convergence_flag,
                }
            )
        return row

    def _cycle_specs(self, fluid: str) -> list[dict[str, Any]]:
        return TRANSCRITICAL_CO2_CYCLE_POINTS if fluid == "CarbonDioxide" else SUBCRITICAL_CYCLE_POINTS

    def _cycle_metrics(self, fluid: str, cycle_spec: dict[str, Any]) -> dict[str, Any] | None:
        if fluid == "CarbonDioxide":
            return self._transcritical_co2_cycle(fluid, cycle_spec)
        return self._subcritical_cycle(fluid, cycle_spec)

    def _subcritical_cycle(self, fluid: str, cycle_spec: dict[str, Any]) -> dict[str, Any] | None:
        te = float(cycle_spec["evaporating_temperature_c"]) + 273.15
        tc = float(cycle_spec["condensing_temperature_c"]) + 273.15
        sh = float(cycle_spec["superheat_k"])
        sc = float(cycle_spec["subcooling_k"])
        eta = float(cycle_spec["compressor_isentropic_efficiency"])

        tcrit = PropsSI("Tcrit", fluid)
        if tc >= tcrit:
            return None

        pe = PropsSI("P", "T", te, "Q", 1, fluid)
        pc = PropsSI("P", "T", tc, "Q", 0, fluid)
        h1 = PropsSI("Hmass", "T", te + sh, "P", pe, fluid)
        s1 = PropsSI("Smass", "T", te + sh, "P", pe, fluid)
        rho1 = PropsSI("Dmass", "T", te + sh, "P", pe, fluid)
        h2s = PropsSI("Hmass", "P", pc, "Smass", s1, fluid)
        h2 = h1 + (h2s - h1) / eta
        h3 = PropsSI("Hmass", "T", tc - sc, "P", pc, fluid)
        h4 = h3
        t2 = PropsSI("T", "P", pc, "Hmass", h2, fluid)

        q_evap = h1 - h4
        w_comp = h2 - h1
        if q_evap <= 0 or w_comp <= 0:
            return None

        case = self._cycle_context(cycle_spec, cycle_model="subcritical_vapor_compression")
        return {
            "cop": q_evap / w_comp,
            "qvol": q_evap * rho1 / 1e6,
            "pressure_ratio": pc / pe,
            "discharge_temperature_c": t2 - 273.15,
            "cycle_label": cycle_spec["case_name"],
            "method": "CoolProp subcritical vapor-compression cycle",
            "status": "resolved:subcritical",
            "cycle_case_id": case["cycle_case_id"],
            "operating_point_hash": case["operating_point_hash"],
            "operating_point_json": case["operating_point_json"],
            "cycle_model": case["cycle_model"],
            "eos_source": case["eos_source"],
        }

    def _transcritical_co2_cycle(self, fluid: str, cycle_spec: dict[str, Any]) -> dict[str, Any] | None:
        te = float(cycle_spec["evaporating_temperature_c"]) + 273.15
        tg = float(cycle_spec["gas_cooler_outlet_temperature_c"]) + 273.15
        ph = float(cycle_spec["high_side_pressure_mpa"]) * 1e6
        sh = float(cycle_spec["superheat_k"])
        eta = float(cycle_spec["compressor_isentropic_efficiency"])

        pe = PropsSI("P", "T", te, "Q", 1, fluid)
        h1 = PropsSI("Hmass", "T", te + sh, "P", pe, fluid)
        s1 = PropsSI("Smass", "T", te + sh, "P", pe, fluid)
        rho1 = PropsSI("Dmass", "T", te + sh, "P", pe, fluid)
        h2s = PropsSI("Hmass", "P", ph, "Smass", s1, fluid)
        h2 = h1 + (h2s - h1) / eta
        h3 = PropsSI("Hmass", "T", tg, "P", ph, fluid)
        h4 = h3
        t2 = PropsSI("T", "P", ph, "Hmass", h2, fluid)

        q_evap = h1 - h4
        w_comp = h2 - h1
        if q_evap <= 0 or w_comp <= 0:
            return None

        case = self._cycle_context(cycle_spec, cycle_model="transcritical_co2")
        return {
            "cop": q_evap / w_comp,
            "qvol": q_evap * rho1 / 1e6,
            "pressure_ratio": ph / pe,
            "discharge_temperature_c": t2 - 273.15,
            "cycle_label": cycle_spec["case_name"],
            "method": "CoolProp transcritical CO2 cycle",
            "status": "resolved:transcritical_co2",
            "cycle_case_id": case["cycle_case_id"],
            "operating_point_hash": case["operating_point_hash"],
            "operating_point_json": case["operating_point_json"],
            "cycle_model": case["cycle_model"],
            "eos_source": case["eos_source"],
        }

    def _cycle_context(self, cycle_spec: dict[str, Any], *, cycle_model: str) -> dict[str, Any]:
        point = {
            "evaporating_temperature_c": cycle_spec.get("evaporating_temperature_c"),
            "condensing_temperature_c": cycle_spec.get("condensing_temperature_c"),
            "gas_cooler_outlet_temperature_c": cycle_spec.get("gas_cooler_outlet_temperature_c"),
            "high_side_pressure_mpa": cycle_spec.get("high_side_pressure_mpa"),
            "superheat_k": cycle_spec.get("superheat_k"),
            "subcooling_k": cycle_spec.get("subcooling_k"),
            "compressor_isentropic_efficiency": cycle_spec.get("compressor_isentropic_efficiency"),
        }
        op_hash, op_json = operating_point_hash(point)
        return {
            "cycle_case_id": str(cycle_spec.get("cycle_case_id", "custom_coolprop_cycle")),
            "cycle_model": cycle_model,
            "eos_source": "CoolProp",
            "case_name": str(cycle_spec.get("case_name", cycle_spec.get("cycle_case_id", "custom_coolprop_cycle"))),
            "operating_point_hash": op_hash,
            "operating_point_json": op_json,
            "source_id": "source_coolprop_session",
            "source_name": f"CoolProp {self.version}",
            "created_by_stage_id": "05",
            "notes": "production CoolProp operating point",
        }
