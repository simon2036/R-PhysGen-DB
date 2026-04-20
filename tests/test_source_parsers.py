from __future__ import annotations

import math

from r_physgen_db.sources.epa_gwp_reference_parser import EPATechnologyTransitionsGWPParser
from r_physgen_db.sources.epa_ods_parser import EPAODSParser
from r_physgen_db.sources.epa_snap_parser import EPASNAPParser
from r_physgen_db.sources.nist_thermo_parser import NISTThermoParser


def test_nist_thermo_parser_extracts_phase_properties() -> None:
    html = """
    <html><body>
    <table>
      <tr><th>Quantity</th><th>Value</th><th>Units</th><th>Method</th><th>Reference</th><th>Comment</th></tr>
      <tr><td>Tboil</td><td>221.4</td><td>K</td><td>N/A</td><td>Ref A</td><td>BS</td></tr>
      <tr><td>Tc</td><td>351.25</td><td>K</td><td>N/A</td><td>Ref B</td><td></td></tr>
      <tr><td>Pc</td><td>5.808</td><td>MPa</td><td>N/A</td><td>Ref C</td><td></td></tr>
    </table>
    <table>
      <tr><th>HvapH (kJ/mol)</th><th>Temperature (K)</th><th>Method</th><th>Reference</th><th>Comment</th></tr>
      <tr><td>20.6</td><td>230.0</td><td>A</td><td>Ref D</td><td>AC</td></tr>
    </table>
    </body></html>
    """
    parsed = NISTThermoParser().parse(html)
    props = {row["property_name"]: row for row in parsed}
    assert round(props["boiling_point_c"]["value_num"], 2) == -51.75
    assert round(props["critical_temp_c"]["value_num"], 2) == 78.10
    assert round(props["critical_pressure_mpa"]["value_num"], 3) == 5.808
    assert round(props["vaporization_enthalpy_kjmol"]["value_num"], 1) == 20.6


def test_epa_ods_parser_extracts_metrics() -> None:
    html = """
    <html><body>
    <table>
      <tr>
        <th>Chemical Name</th><th>Lifetime, in years</th><th>ODP1 (Montreal Protocol)</th>
        <th>ODP2 (WMO 2011)</th><th>GWP1 (AR4)</th><th>GWP2 (AR5)</th><th>CAS Number</th>
      </tr>
      <tr>
        <td>Trichlorofluoromethane</td><td>45</td><td>1.0</td><td>1.0</td><td>4,750</td><td>4,660</td><td>75-69-4</td>
      </tr>
    </table>
    </body></html>
    """
    df = EPAODSParser().parse(html)
    row = df.iloc[0]
    assert row["chemical_name"] == "Trichlorofluoromethane"
    assert row["cas_number"] == "75-69-4"
    assert row["gwp_ar4_100yr"] == 4750.0
    assert row["odp_montreal_protocol"] == 1.0


def test_epa_snap_parser_extracts_regulatory_fields() -> None:
    html = """
    <html><body>
    <table>
      <tr>
        <th>Substitute</th><th>Trade Name(s)</th><th>Retrofit/ New</th><th>ODP value</th>
        <th>GWP value</th><th>ASHRAE Designation (Safety Classification)</th>
        <th>SNAP Listing Date</th><th>Listing Status</th>
      </tr>
      <tr>
        <td>R-290 (Propane)</td><td></td><td>N</td><td>0</td><td>3.3</td><td>A3</td><td>July 2015</td>
        <td>Acceptable subject to use conditions: See rule for detailed conditions.</td>
      </tr>
    </table>
    </body></html>
    """
    df = EPASNAPParser().parse(html, end_use="commercial_ice_machines")
    row = df.iloc[0]
    assert row["substitute"] == "R-290 (Propane)"
    assert row["acceptability"] == "acceptable_subject_to_use_conditions"
    assert row["gwp"] == 3.3
    assert row["ashrae_safety"] == "A3"


def test_epa_gwp_reference_parser_extracts_exact_values_and_marks_ranges() -> None:
    html = """
    <html><body>
    <table>
      <tr>
        <th>Substance Name</th><th>100-Year Global Warming Potential</th><th>Reference</th>
      </tr>
      <tr>
        <td>Acetone</td><td>0.5</td><td>IPCC 2007</td>
      </tr>
      <tr>
        <td>Blends of 10% to 90% HFO-1234ze(E) by weight and the remainder HCFO-1233zd(E)</td>
        <td>1.3-3.7</td><td>Calculated</td>
      </tr>
    </table>
    </body></html>
    """
    df = EPATechnologyTransitionsGWPParser().parse(html)
    exact = df.loc[df["substance_name"] == "Acetone"].iloc[0]
    ranged = df.loc[df["substance_name"].str.startswith("Blends of 10% to 90% HFO-1234ze(E)")].iloc[0]
    assert exact["gwp_100yr"] == 0.5
    assert math.isnan(exact["gwp_range_min"])
    assert math.isnan(exact["gwp_range_max"])
    assert bool(ranged["is_range"]) is True
    assert math.isnan(ranged["gwp_100yr"])
    assert ranged["gwp_range_min"] == 1.3
    assert ranged["gwp_range_max"] == 3.7
