import logging
from datetime import datetime, timedelta

import pandas as pd
from dateutil import rrule as rr

start = datetime(2023, 1, 1)
end = datetime(2023, 3, 31)
index = pd.date_range(
    start=start,
    end=end + timedelta(hours=24),
    freq="h",
)

print(index.size-1)
