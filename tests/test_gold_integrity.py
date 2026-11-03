from pathlib import Path
import pandas as pd
import pytest
import numpy as np

BASE_DIR = Path(__file__).resolve().parent.parent #always point to the root of the repo
GOLD_PATH = BASE_DIR / "data/gold/power_market_15min"
@pytest.fixture(scope="module")
def gold_df():
    """
    Fixture to load the Gold dataset once for all tests in this module.
    """
    return pd.read_parquet(GOLD_PATH)

required_cols = [
    "start_time_utc",
    "price_eur_per_mwh",
    "wind_mw",
    "nuclear_mw",
    "hydro_mw",
    "consumption_mw"
]
def test_gold_no_null_values(gold_df):
    """
    Test that the gold data has no null values.
    """
    df = gold_df
    for col in required_cols:
        null_rows = df[df[col].isnull()]
        if len(null_rows) > 0:
            bad_dates = null_rows["start_time_utc"].tolist()
            assert (
                len(null_rows) == 0
            ), f"Column '{col}' has null values at dates: {bad_dates}"

def test_dst_transition(gold_df):
    """
    Test that the summertime transition is handled correctly. 
    There should be 96 rows per day, except for the DST transition days which should have 100 rows.
    Why? : 15-minute intervals in a day = 24 hours * 4 intervals/hour = 96 intervals.
    15-minute intervals in a day with DST transition = 24 hours * 4 intervals/hour + 4 extra intervals (1 hour) = 100 intervals.
    """
    df = gold_df
    # 1. Spring forward: 23 hours -> 92 intervals
    spring_day = df[df["date_start_helsinki"].astype(str) == "2026-03-29"]
    assert len(spring_day) == 92, f"Spring DST transition day must have 92 rows, got {len(spring_day)} rows."
    # 2. Fall back: 25 hours -> 100 intervals
    fall_day = df[df["date_start_helsinki"].astype(str) == "2026-10-25"]
    assert len(fall_day) == 100, f"Fall DST transition day must have 100 rows, got {len(fall_day)} rows."
    # 3. Regular normal day: 24 hours -> 96 intervals
    normal_day = df[df["date_start_helsinki"].astype(str) == "2026-10-24"]
    assert len(normal_day) == 96, f"Normal day must have 96 rows, got {len(normal_day)} rows."

def test_power_balance_math_consistency(gold_df):
    """
    Test that calculated metric columns match the underlying raw columns:
    1. selected_generation_mw == wind_mw + nuclear_mw + hydro_mw
    2. net_load_ex_solar_mw == consumption_mw - wind_mw
    3. selected_gen_balance_mw == selected_generation_mw - consumption_mw
    """
    df = gold_df
    # 1. Total selected generation check
    expected_gen = df["wind_mw"] + df["nuclear_mw"] + df["hydro_mw"]
    assert np.allclose(
        expected_gen, df["selected_generation_mw"], atol=1e-2
    ), "selected_generation_mw does not match sum of wind, nuclear, and hydro."
    # 2. Net load check
    expected_net_load = df["consumption_mw"] - df["wind_mw"]
    assert np.allclose(
        expected_net_load, df["net_load_ex_solar_mw"], atol=1e-2
    ), "net_load_ex_solar_mw does not match consumption minus wind."
    # 3. Generation balance check
    expected_balance = df["selected_generation_mw"] - df["consumption_mw"]
    assert np.allclose(
        expected_balance, df["selected_gen_balance_mw"], atol=1e-2
    ), "selected_gen_balance_mw does not match generation minus consumption."

def test_timestamp_monotonic_and_unique(gold_df):

    df = gold_df
    # 1. Check that start_time_utc is unique (no duplicates)
    assert df["start_time_utc"].is_unique, "Duplicate start_time_utc found in Gold table."
    # 2. Check that start_time_utc is strictly increasing (monotonic)
    assert df["start_time_utc"].is_monotonic_increasing, "Timestamps are not strictly sorted."

def test_temporal_feature_integrity(gold_df):

    df = gold_df
    # Check that minute_start_helsinki only contains valid 15-minute intervals
    valid_minutes = {0, 15, 30, 45}
    assert set(df["minute_start_helsinki"].unique()).issubset(valid_minutes), \
        "Invalid minutes found. Expected [0, 15, 30, 45]."
    
    # Check that day_of_week matches the actual day of the week for date_start_helsinki
    weekend_subset = df[df["is_weekend"] == True]
    assert weekend_subset["day_of_week"].isin([1, 7]).all(), \
        "Weekend flag does not match Saturday(7) or Sunday(1) in Spark SQL format"

def test_physical_limits_and_pricing(gold_df):

    df = gold_df
    # Check that generation and consumption values are non-negative
    assert (df["wind_mw"] >= 0).all(), "Wind generation cannot be negative."
    assert (df["nuclear_mw"] >= 0).all(), "Nuclear generation cannot be negative."
    assert (df["hydro_mw"] >= 0).all(), "Hydro generation cannot be negative."
    assert (df["consumption_mw"] >= 0).all(), "Consumption cannot be negative."
    
    # Check that price values are within the Nord Pool clearing price limits (-500 to 4000 EUR/MWh)
    # Price limits are based on EU ACER / Nord Pool SDAC regulations (HMMCP)
    assert (df["price_eur_per_mwh"] > -500).all() and (df["price_eur_per_mwh"] < 4000).all(), \
        "Price contains values outside the Nord Pool clearing price limits."