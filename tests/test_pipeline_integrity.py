from pathlib import Path
import sys
import pandas as pd
import pytest

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

from common import bq_queries, file_config

RESOURCES_DIR = BASE_DIR / "resources"
UNIT_SILVER_DIR = BASE_DIR / "data/silver/entsoe/unit_generation"
FUEL_SILVER_DIR = BASE_DIR / "data/silver/entsoe/fuel_generation"


def test_resources_primary_keys_unique():
    """Verify every static dimension CSV exists and has unique, non-null primary keys."""
    for item in file_config.initialise_dimension_files_config_list:
        csv_path = item["local_file"]
        pk_col = item["pk"]
        assert csv_path.exists(), f"Missing reference CSV: {csv_path}"
        df = pd.read_csv(csv_path)
        assert len(df) > 0, f"Empty reference CSV: {csv_path}"
        assert pk_col in df.columns, f"Missing PK column {pk_col} in {csv_path.name}"
        assert df[pk_col].notna().all(), f"Null PK values found in {csv_path.name}"
        assert df[pk_col].is_unique, f"Duplicate PK values found in {csv_path.name}"


def test_resources_referential_integrity():
    """Verify foreign keys across power units, fuel mappings, cities, and regions."""
    units = pd.read_csv(RESOURCES_DIR / "fi_power_units_master.csv")
    fuels = pd.read_csv(RESOURCES_DIR / "psr_fuel_type_mapping.csv")
    cities = pd.read_csv(RESOURCES_DIR / "fi_cities_coordinates.csv")
    regions = pd.read_csv(RESOURCES_DIR / "fi_regions_coordinates.csv")

    missing_fuels = set(units["entsoe_psr_code"]) - set(fuels["entsoe_psr_code"])
    assert not missing_fuels, f"Orphan entsoe_psr_code in power units: {missing_fuels}"

    active_units = units[units["is_active"] == True]
    missing_cities = set(active_units["city_ascii"]) - set(cities["city_ascii"])
    missing_regions = set(active_units["region_fi"]) - set(regions["region_fi"])
    assert not missing_cities, f"Orphan active city_ascii keys: {missing_cities}"
    assert not missing_regions, f"Orphan active region_fi keys: {missing_regions}"


def test_common_config_and_bq_queries_valid():
    """Verify central YAML config and BigQuery SQL templates load cleanly."""
    assert file_config.GCP_PROJECT_ID
    assert file_config.GCS_BUCKET_NAME
    assert file_config.BQ_DATASET_NAME
    assert len(bq_queries.CREATE_FACT_TABLES_SQL) == 5
    assert len(bq_queries.CREATE_ANALYTICAL_VIEWS_SQL) == 3
    for sql in bq_queries.CREATE_ANALYTICAL_VIEWS_SQL:
        assert "CREATE OR REPLACE VIEW" in sql


def test_silver_unit_generation_integrity():
    """Verify Silver A73 unit generation Parquet has no negative MW, duplicates, or unknown units."""
    if not UNIT_SILVER_DIR.exists():
        pytest.skip("Local Silver unit_generation Parquet not present in CI runner")

    df = pd.read_parquet(UNIT_SILVER_DIR)
    units = pd.read_csv(RESOURCES_DIR / "fi_power_units_master.csv")

    assert len(df) > 0, "Silver unit_generation table is empty"
    assert (df["generation_mw"] >= 0).all(), "Negative generation_mw detected in A73 Silver"
    assert not df.duplicated(subset=["start_time_utc", "unit_mrid"]).any(), "Duplicate (timestamp, unit_mrid) rows"

    unknown_units = set(df["unit_mrid"]) - set(units["unit_mrid"])
    assert not unknown_units, f"Unmapped unit_mrid in Silver A73: {unknown_units}"


def test_silver_fuel_generation_integrity():
    """Verify Silver A75 fuel generation Parquet has no negative MW, duplicates, or unknown PSR codes."""
    if not FUEL_SILVER_DIR.exists():
        pytest.skip("Local Silver fuel_generation Parquet not present in CI runner")

    df = pd.read_parquet(FUEL_SILVER_DIR)
    fuels = pd.read_csv(RESOURCES_DIR / "psr_fuel_type_mapping.csv")

    assert len(df) > 0, "Silver fuel_generation table is empty"
    assert (df["generation_mw"] >= 0).all(), "Negative generation_mw detected in A75 Silver"
    assert not df.duplicated(subset=["start_time_utc", "entsoe_psr_code"]).any(), "Duplicate (timestamp, psr) rows"

    unknown_psr = set(df["entsoe_psr_code"]) - set(fuels["entsoe_psr_code"])
    assert not unknown_psr, f"Unmapped entsoe_psr_code in Silver A75: {unknown_psr}"


def test_unit_vs_fuel_physical_consistency():
    """Verify nuclear A73 sum matches A75 exactly and total A73 never exceeds total A75."""
    if not UNIT_SILVER_DIR.exists() or not FUEL_SILVER_DIR.exists():
        pytest.skip("Local Silver Parquet files not present in CI runner")

    unit_df = pd.read_parquet(UNIT_SILVER_DIR)
    fuel_df = pd.read_parquet(FUEL_SILVER_DIR)

    # Nuclear (B14) is single-fuel and 100% covered by A73 units
    nuc_unit = (
        unit_df[unit_df["entsoe_psr_code"] == "B14"]
        .groupby("start_time_utc", as_index=False)["generation_mw"]
        .sum()
        .rename(columns={"generation_mw": "unit_nuc_mw"})
    )
    nuc_fuel = fuel_df[fuel_df["entsoe_psr_code"] == "B14"][["start_time_utc", "generation_mw"]]
    nuc_merged = nuc_unit.merge(nuc_fuel, on="start_time_utc", how="inner")
    assert len(nuc_merged) > 0, "No overlapping timestamps for Nuclear B14"
    nuc_rel_diff = ((nuc_merged["unit_nuc_mw"] - nuc_merged["generation_mw"]).abs() / nuc_merged["generation_mw"].clip(lower=1.0)).max()
    assert nuc_rel_diff < 0.01, f"Nuclear B14 A73 vs A75 mismatch: max relative diff {nuc_rel_diff:.4%}"

    # Total A73 across all units must never exceed total national A75 generation
    tot_unit = unit_df.groupby("start_time_utc", as_index=False)["generation_mw"].sum().rename(columns={"generation_mw": "tot_a73"})
    tot_fuel = fuel_df.groupby("start_time_utc", as_index=False)["generation_mw"].sum().rename(columns={"generation_mw": "tot_a75"})
    tot_merged = tot_unit.merge(tot_fuel, on="start_time_utc", how="inner")
    assert (tot_merged["tot_a73"] <= tot_merged["tot_a75"]).all(), "Total A73 exceeded national A75 generation"
