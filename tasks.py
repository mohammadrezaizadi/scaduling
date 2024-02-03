import matplotlib.pyplot as plt
import pandas as pd
import random
import numpy as np

M = 8

def create_tasks(dataSize=100, maxSubtaskSize=20):
    SubTasks = pd.DataFrame(
        columns=["taskID", "SubTaskID", "SubTaskRealID", "duration", "parentId", 'Li', 'startTime', 'endTime'])

    for i in range(dataSize):
        subtasksNumber = int(random.randint(5, maxSubtaskSize))
        for j in range(subtasksNumber):
            duration = random.randint(50, 100)
            parentId = ''
            parentsCount = random.randint(0, j + 3)
            if parentsCount == 0 or parentsCount >= j:
                parentId = '-1'

            if parentId != '-1':
                parentsCount = parentsCount if parentsCount < j else random.randint(0, j)
                parent_ids = set(SubTasks.loc[SubTasks['taskID'] == i]['SubTaskID'].sample(parentsCount))
                parentId += ',' + ','.join(str(pid) for pid in parent_ids)

            sutId = i * 10 + j if j < 9 else i * 100 + j
            row = {"taskID": i, "SubTaskID": sutId, "SubTaskRealID": j, "duration": duration, "parentId": parentId,
                   'Li': duration}
            SubTasks = SubTasks._append(row, ignore_index=True)

    tasks = pd.DataFrame(columns=['TaskID', "c", "L", "T", "D", 'U', 'DeadLine', 'alpha'])
    for i in range(dataSize):
        lst = SubTasks.loc[SubTasks['taskID'] == i]
        Ci = int(sum(lst.duration))
        Li = int(max(lst.Li))
        Ti = Di = int(random.randint(Li, Li + 2000))
        Ui = float(Ci) / float(Ti)
        gamma_value = np.random.gamma(2, 1)
        DL = Li + Ci + 0.5 * Ui * (1 + 0.25 * gamma_value)
        al = Ci / (Ti - (M / (M + 1) * Li))
        row = {'TaskID': int(i), "c": Ci, 'L': Li, 'T': Ti, 'D': Di, 'U': Ui, 'DeadLine': DL, 'alpha': al}
        tasks = tasks._append(row, ignore_index=True)
    # محاسبه طول زمانی برای هر subtask
    # SubTasks['Li'] = 0
    for i in range(len(tasks)):
        s = SubTasks.loc[SubTasks['taskID'] == i]
        for _, row in s.iterrows():
            parent_ids = [int(p) for p in row['parentId'].split(',') if p != '']
            if (parent_ids[0] != -1):
                parent_duration = s[s['SubTaskID'].isin(parent_ids)]['Li'].max()
                SubTasks.loc[(SubTasks['taskID'] == i) & (
                        SubTasks['SubTaskRealID'] == row['SubTaskRealID']), 'Li'] = parent_duration + row[
                    'duration']
                SubTasks.loc[(SubTasks['taskID'] == i) & (
                        SubTasks['SubTaskRealID'] == row['SubTaskRealID']), 'startTime'] = parent_duration
                SubTasks.loc[(SubTasks['taskID'] == i) & (
                        SubTasks['SubTaskRealID'] == row['SubTaskRealID']), 'endTime'] = parent_duration + row[
                    'duration']
            else:
                SubTasks.loc[
                    (SubTasks['taskID'] == i) & (SubTasks['SubTaskRealID'] == row['SubTaskRealID']), 'startTime'] = 0
                SubTasks.loc[(SubTasks['taskID'] == i) & (
                        SubTasks['SubTaskRealID'] == row['SubTaskRealID']), 'endTime'] = row['duration']
    # محاسبه طولانی‌ترین مقدار Li برای هر task
    tasks['L'] = SubTasks.groupby('taskID')['Li'].max().reset_index()['Li']
    return tasks, SubTasks


def create_segments(tasks, subTasks):
    tasks = tasks.assign(Segment='0,', SegmentCount=0, Mij='', Sij='')
    for _, task in tasks.iterrows():
        st = subTasks.loc[subTasks['taskID'] == task.TaskID]
        segments = []
        for _, subtask in st.iterrows():
            segments = [int(p) for p in tasks.loc[tasks.TaskID == task.TaskID, 'Segment'].values[0].split(',') if
                        p != '']
            if subtask.startTime not in segments:
                tasks.at[task.TaskID, 'Segment'] += str(subtask.startTime) + ','
            if subtask.endTime not in segments:
                tasks.at[task.TaskID, 'Segment'] += str(subtask.endTime) + ','
        segments = [int(p) for p in tasks.loc[tasks.TaskID == task.TaskID, 'Segment'].values[0].split(',') if
                    p != '']
        segments = sorted(segments)
        result = ','.join(map(str, segments))
        tasks.at[task.TaskID, 'Segment'] = result

        for i in range(1, len(segments)):
            StartTimeOfSegment = segments[i - 1]
            EndTimeOfSegment = segments[i]
            sij = EndTimeOfSegment - StartTimeOfSegment
            mij = len(st.loc[(st['startTime'] <= StartTimeOfSegment) & (st['endTime'] >= EndTimeOfSegment)])
            tasks.at[task.TaskID, 'Mij'] += str(mij) + ","
            tasks.at[task.TaskID, 'Sij'] += str(sij) + ","

        tasks.at[task.TaskID, 'SegmentCount'] = len(segments) - 1

    return tasks


def exe_DAGFluid(tasks):
    tasks = tasks.assign(HeavyOrLight='', SqqOrParal='', tetaij=0, Li_light=-1, Ci_heavy=-1)

    HOrL = []
    for _, task in tasks.iterrows():
        Mij = [int(p) for p in tasks.loc[tasks.TaskID == task.TaskID, 'Mij'].values[0].split(',') if
               p != '']
        Sij = [int(p) for p in tasks.loc[tasks.TaskID == task.TaskID, 'Sij'].values[0].split(',') if
               p != '']
        HOrL = [0 if val <= task.alpha else 1 for val in Mij]
        zero_count = HOrL.count(0)
        one_count = HOrL.count(1)
        result = ','.join(map(str, HOrL))
        tasks.at[task.TaskID, 'HeavyOrLight'] = result
        tasks.at[task.TaskID, 'SqqOrParal'] = 's' if task.U <= 1 else 'p'
        # اگر تسک سکونشال بود نرخ اجرا میشود u
        tasks.at[task.TaskID, 'tetaij'] = task.U if task.U <= 1 else -1
        Li_light = -1
        Ci_heavy = -1
        # اینجا اگر یک تسک پارالل بود
        if (task.U > 1):
            for i in range(len(HOrL)):
                # اگر سگمنت لایت بود
                if HOrL[i] == 0:
                    Li_light += Sij[i]
                else:
                    Ci_heavy += Mij[i] * Sij[i]
            tasks.at[task.TaskID, 'Li_light'] = Li_light
            tasks.at[task.TaskID, 'Ci_heavy'] = Ci_heavy

            lst = ''
            # اگر همه سگمنت ها سبک بود
            if zero_count == len(HOrL):
                # یک لیست به تعداد سگمنت ها همه نرخ اجرای یک میگیرد
                lst = ','.join(map(str, ([1] * len(HOrL))))
            # اگر همه سگمنت ها سنگین بود
            elif one_count == len(HOrL):
                # نرخ اجرای سگمنت ها وقتی همه سنگین باشند
                for i in range(len(Mij)):
                    lst += str((task.c / (task.D * Mij[i]))) + ','

            # اگر سگمنت ها سبک و سنگین بود
            else:
                # اینجا هم در هر تسک میگیم سگمنت هاش چه وضعیتی دارن
                for i in range(len(HOrL)):
                    tmp = (M / (M + 1)) * task.L
                    if Li_light <= tmp:
                        if HOrL[i] == 0:
                            lst += '1,'
                        else:
                            lst += str(Ci_heavy / (task.D - (M / (M + 1)) * float(task.L) ) * float(Mij[i])) + ','
                    else:
                        if HOrL[i] == 0:
                            lst += '1,'
                        else:
                            if task.L <= ((M + 1) / M) * (task.D - ((1 / M) * task.c)):
                                lst += str(Ci_heavy / (1 - (((M / (M + 1)) * task.L) / task.c)) * (
                                            task.T - (M / (M + 1)) * task.L) * Mij[i]) + ','
                            else:
                                lst += str((task.L -Li_light)/(task.T - Li_light)) + ','

            tasks.at[task.TaskID, 'tetaij'] = lst

    # تست کل
    tmp = sum(tasks.loc[tasks.SqqOrParal == 's' , 'U'])
    max = -1
    for _, task in tasks.iterrows():
        if task.SqqOrParal == 'p':
            Mij = [int(p) for p in tasks.loc[tasks.TaskID == task.TaskID, 'Mij'].values[0].split(',') if
                   p != '']
            teta = [float(p) for p in tasks.loc[tasks.TaskID == task.TaskID, 'tetaij'].values[0].split(',') if
                   p != '']
            for i in range(len(Mij)):
                max = max if max< Mij[i] * teta[i] else  Mij[i] * teta[i]

    test_result = max + tmp <=M




    return tasks , test_result


def Namayesh_dag(SubTasks, Tasks):
    # استخراج داده‌های مورد نیاز از دیتافریم‌ها
    task_data = Tasks[['TaskID', 'L', 'Segment']]
    subtask_data = SubTasks[['SubTaskID', 'taskID', 'duration', 'parentId', 'startTime']]

    # محاسبه زمان شروع هر تسک
    start_times = [0] * len(task_data)
    for i in range(1, len(task_data)):
        start_times[i] = task_data.iloc[i - 1]['L'] + start_times[i - 1]

    fig, ax = plt.subplots(figsize=(20, 10))
    plt.rcParams['font.size'] = 12
    y = 1
    for i, row in task_data.iterrows():
        task_id = row['TaskID']
        x = start_times[i]
        y += 4
        duration = Tasks[Tasks['TaskID'] == task_id]['L'].values[0]
        ax.barh(y, duration, left=x, align='center', height=0.4, color='blue')
        ax.text(x + duration / 2, y, f'Task {task_id}', ha='center', va='center')

        subtasks = subtask_data[subtask_data['taskID'] == task_id]

        for j, subtask_row in subtasks.iterrows():
            ax.barh(y - 0.4, subtask_row['duration'], left=x + subtask_row['startTime'], align='center', height=0.1,
                    color='green')
            y -= 0.12
        vls = [int(p) for p in row['Segment'].split(',') if p != '']
        for k in range(len(vls)):
            ax.vlines(x + vls[k], y - 1.2, y + 1.2, colors='red', linestyles='dashed', linewidth=0.2)

    ax.set_xlabel('Time')
    ax.set_ylabel('Tasks')
    ax.set_title('tasks')
    plt.show()


tasks, subTasks = create_tasks(100)

tasks = create_segments(tasks, subTasks)

tasks , res = exe_DAGFluid(tasks)
tasks.to_csv('tasks.csv')
subTasks.to_csv('Subtasks.csv')

print(tasks, "\n", subTasks)
print('DAG-FLUID Result =====>' , res)
Namayesh_dag(subTasks, tasks)
