import random

import pandas as pd
import numpy as np
# from builtins import round


def create_data(dataSize = 50):
    data = np.zeros((dataSize, 3))

    data[:, 0] = np.arange(1, dataSize +1, dtype=int)

    data[:, 1] = np.random.uniform(0.001, 1, size=dataSize)
    for i in range(len(data)):
        data[i][1] = round(data[i][1] , 3)

    parent_indices = np.random.choice(range(5, dataSize), size=10, replace=False)
    for i in parent_indices:
        r = int(np.random.uniform(1, 5))
        parent_id = int(np.mean(data[i - r:i, 0]))
        data[i, 2] = parent_id

    df = pd.DataFrame(data, columns=["taskNum", "duration", "parentId"])
    return df

# استفاده از متد create_data برای ایجاد داده
data_frame = create_data(100)

# نمایش داده
print(data_frame)