import pandas as pd


str = '1,2,4,4,5,5,3,3,5,8,9,6,10,12,'
nums = str[:-1].split(',')
nums = [int(x) for x in nums]
print(nums)
print(set(nums))



