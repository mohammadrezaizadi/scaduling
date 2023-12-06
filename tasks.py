import random

import pandas as pd
import numpy as np
# from builtins import round


def create_data():
    data = np.zeros((50, 3))  # ساخت یک آرایه صفر با ابعاد 50x3

    # پر کردن ستون اول با شماره سطر به صورت float
    data[:, 0] = np.arange(1, 51, dtype=int)

    # پر کردن ستون دوم با اعداد رندم بین 0.00001 تا 1
    data[:, 1] = np.random.uniform(0.001, 1, size=50)
    for i in range(len(data)):
        data[i][1] = round(data[i][1] , 3)

    # پر کردن ستون سوم با انتخاب تصادفی یک عنصر از هر 5 سطر
    parent_indices = np.random.choice(range(5, 50), size=10, replace=False)
    for i in parent_indices:
        r = int(np.random.uniform(1, 5))
        parent_id = int(np.mean(data[i - r:i, 0]))
        data[i, 2] = parent_id


# data[:, 2] = parent_indices

    # تبدیل آرایه به DataFrame با نام ستون‌های مشخص
    df = pd.DataFrame(data, columns=["taskNum", "duration", "parentId"])
    return df

# استفاده از متد create_data برای ایجاد داده
data_frame = create_data()

# نمایش داده
print(data_frame)