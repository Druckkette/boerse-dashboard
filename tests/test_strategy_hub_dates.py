import pandas as pd

def test_datetime_index_comparison_with_buy_date_is_valid():
    idx = pd.date_range('2026-05-01', periods=5, tz='UTC')
    df = pd.DataFrame({'open':[1]*5,'high':[1]*5,'low':[1]*5,'close':[1]*5,'volume':[1]*5}, index=idx)
    buy_date = pd.Timestamp('2026-05-03').tz_localize(None)
    df.index = pd.to_datetime(df.index).tz_localize(None)
    filtered = df[df.index >= buy_date]
    assert len(filtered) == 3
