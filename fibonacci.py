# import multiprocessing
#
# def fibonacci(n):
#     if n <= 0:
#         return 0
#     elif n == 1:
#         return 1
#     else:
#         return fibonacci(n-1) + fibonacci(n-2)
#
# def calculate_fibonacci(index):
#     result = fibonacci(index)
#     cpu_number = multiprocessing.current_process().name
#     print(f"Fibonacci({index}) = {result} (CPU: {cpu_number})")
#
# if __name__ == '__main__':
#     num_processors = multiprocessing.cpu_count()
#     print(num_processors)
#     pool = multiprocessing.Pool(processes=num_processors)
#     tasks = range(40)
#     pool.map(calculate_fibonacci, tasks)
#     pool.close()
#     pool.join()


import multiprocessing
import math

def fibonacci(n):
    if n <= 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fibonacci(n-1) + fibonacci(n-2)

def execute_task(task, execution_rate):
    result = fibonacci(task)
    cpu_number = multiprocessing.current_process().name
    print(f"Task {task} executed with execution rate {execution_rate} on CPU {cpu_number}")
    # انجام عملیات دیگر با نتیجه حاصل از اجرای وظیفه

def schedule_sequential_tasks(tasks):
    for task in tasks:
        execute_task(task, 1)  # نرخ اجرای ثابت برای وظایف ترتیبی

def schedule_parallel_tasks(tasks, segment_execution_rates):
    for i, task in enumerate(tasks):
        segments = decompose_task(task)
        for j, segment in enumerate(segments):
            execution_rate = segment_execution_rates[i][j]
            for thread in segment:
                execute_task(thread, execution_rate)  # نرخ اجرای مشخص برای وظایف همزمان


def decompose_task(task):
    segments = []
    if task <= 1:
        segments.append([task])  # وظیفه کوچکتر یا مساوی 1 بدون تجزیه به بخش‌ها اجرا می‌شود
    else:
        segments = divide_task(task)
    return segments

def divide_task(task):
    segments = []
    Ui = 3  # نرخ اجرا بر ثانیه

    # محاسبه تعداد بخش‌ها بر اساس نرخ اجرا
    num_segments = math.ceil(task / Ui)
    segment_size = math.ceil(task / num_segments)
    remaining = task

    for _ in range(num_segments):
        segment = []
        if remaining >= segment_size:
            # بخش کامل از وظیفه را به زیرظیفه‌ها تخصیص دهید
            for _ in range(segment_size):
                segment.append(remaining)
                remaining -= 1
        else:
            # زیرظیفه‌های باقیمانده را به بخش اضافه کنید
            for _ in range(remaining):
                segment.append(remaining)
                remaining -= 1
        segments.append(segment)

    return segments

if __name__ == '__main__':
    num_processors = multiprocessing.cpu_count()
    pool = multiprocessing.Pool(processes=num_processors)
    tasks = range(40)
    segment_execution_rates = [[0.8, 0.8, 0.6], [0.9, 0.7], [1.0]]  # نرخ اجرای بخش‌ها برای وظایف همزمان
    schedule_sequential_tasks(tasks)  # اجرای وظایف ترتیبی
    schedule_parallel_tasks(tasks, segment_execution_rates)  # اجرای وظایف همزمان
    pool.close()
    pool.join()