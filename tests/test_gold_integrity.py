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

def _last_sunday(year: int, month: int):
    """
    Return the date of the last Sunday for a given year and month.
    """
    last_day = pd.Timestamp(year=year, month=month, day=31)
    offset = (last_day.weekday() + 1) % 7
    return (last_day - pd.Timedelta(days=offset)).date()

def test_dst_transition(gold_df):
    """
    Test that the summertime transition is handled correctly across any date range.
    Daily count must match expected intervals for normal, boundary, and transition dates.
    """
    df = gold_df
    daily_counts = df.groupby("date_start_helsinki").size().sort_index()

    # Check all complete interior days against calendar transition rules
    interior_counts = daily_counts.iloc[1:-1]
    for dt_val, count in interior_counts.items():
        spring_dst = _last_sunday(dt_val.year, 3)
        fall_dst = _last_sunday(dt_val.year, 10)
        if dt_val == spring_dst:
            assert count == 92, f"Spring DST day {dt_val} must have 92 rows, got {count}."
        elif dt_val == fall_dst:
            assert count == 100, f"Fall DST day {dt_val} must have 100 rows, got {count}."
        else:
            assert count == 96, f"Normal day {dt_val} must have 96 rows, got {count}."

    # Verify full daily distribution including boundary dates against timezone projection
    start_utc = pd.Timestamp(df["start_time_utc"].min()).tz_localize("UTC")
    end_utc = pd.Timestamp(df["start_time_utc"].max()).tz_localize("UTC")
    ref_helsinki = pd.date_range(start_utc, end_utc, freq="15min").tz_convert("Europe/Helsinki")
    expected_counts = pd.Series(ref_helsinki.date).value_counts().sort_index()
    assert daily_counts.equals(expected_counts), "Daily interval counts do not match Europe/Helsinki timezone rules."

def test_power_balance_math_consistency(gold_df):
    """
    Test that calculated metric columns match the underlying raw columns.
    """
    df = gold_df
    # Total selected generation check
    expected_gen = df["wind_mw"] + df["nuclear_mw"] + df["hydro_mw"]
    gen_mask = np.isclose(expected_gen, df["selected_generation_mw"], atol=1e-2)
    assert gen_mask.all(), (
        f"selected_generation_mw mismatch at timestamps: {df.loc[~gen_mask, 'start_time_utc'].tolist()}"
    )
    # Net load check
    expected_net_load = df["consumption_mw"] - df["wind_mw"]
    net_mask = np.isclose(expected_net_load, df["net_load_ex_solar_mw"], atol=1e-2)
    assert net_mask.all(), (
        f"net_load_ex_solar_mw mismatch at timestamps: {df.loc[~net_mask, 'start_time_utc'].tolist()}"
    )
    # Generation balance check
    expected_balance = df["selected_generation_mw"] - df["consumption_mw"]
    bal_mask = np.isclose(expected_balance, df["selected_gen_balance_mw"], atol=1e-2)
    assert bal_mask.all(), (
        f"selected_gen_balance_mw mismatch at timestamps: {df.loc[~bal_mask, 'start_time_utc'].tolist()}"
    )

def test_timestamp_monotonic_and_unique(gold_df):

    df = gold_df
    # Check that timestamp is unique
    dup_ts = df.loc[df["start_time_utc"].duplicated(), "start_time_utc"].tolist()
    assert df["start_time_utc"].is_unique, f"Duplicate start_time_utc found at: {dup_ts}"
    # Check that timestamp is strictly increasing
    assert df["start_time_utc"].is_monotonic_increasing, "Timestamps are not strictly sorted."

def test_temporal_feature_integrity(gold_df):

    df = gold_df
    # Check that minute column contains valid quarter intervals
    valid_minutes = {0, 15, 30, 45}
    assert set(df["minute_start_helsinki"].unique()).issubset(valid_minutes), \
        "Invalid minutes found. Expected [0, 15, 30, 45]."
    
    # Check that day of week matches the actual weekend flag
    weekend_subset = df[df["is_weekend"] == True]
    assert weekend_subset["day_of_week"].isin([1, 7]).all(), \
        "Weekend flag does not match Saturday(7) or Sunday(1) in Spark SQL format"

def test_physical_limits_and_pricing(gold_df):

    df = gold_df
    # Check that generation and consumption values are non-negative
    for col in ["wind_mw", "nuclear_mw", "hydro_mw", "consumption_mw"]:
        neg_ts = df.loc[df[col] < 0, "start_time_utc"].tolist()
        assert len(neg_ts) == 0, f"Column '{col}' has negative values at timestamps: {neg_ts}"
    
    # Check price bounds against market clearing limits
    bad_price_ts = df.loc[
        (df["price_eur_per_mwh"] <= -500) | (df["price_eur_per_mwh"] >= 4000),
        "start_time_utc"
    ].tolist()
    assert len(bad_price_ts) == 0, (
        f"Price outside clearing limits at timestamps: {bad_price_ts}"
    )

def test_timestamp_continuity(gold_df):
    """
    Test that time intervals have no gaps and match the full expected length between start and end bounds.
    """
    df = gold_df
    start_utc = df["start_time_utc"].min()
    end_utc = df["start_time_utc"].max()
    expected_rows = int((end_utc - start_utc) / pd.Timedelta(minutes=15)) + 1
    assert len(df) == expected_rows, f"Expected {expected_rows} rows between {start_utc} and {end_utc}, got {len(df)}"
    diffs = df["start_time_utc"].diff().dropna()
    gap_indices = diffs.index[diffs != pd.Timedelta(minutes=15)]
    gap_ts = df.loc[gap_indices, "start_time_utc"].tolist()
    assert len(gap_ts) == 0, f"Found time gaps in Gold table at timestamps: {gap_ts}"
