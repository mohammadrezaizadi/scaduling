import random
import pandas as pd
import numpy as np


def create_tasks(dataSize=100):
    SubTasks = pd.DataFrame(columns=["taskID", "SubTaskID", "duration", "parentId", 'Li'])

    for i in range(dataSize):
        subtasksNumber = int(random.randint(4, 10))
        # parent = int(random.randint(1, subtasks))
        for j in range(subtasksNumber):
            duration = random.randint(10, 2000)
            # parent = int(random.randint(-1, j - 1))
            # parentId = -1 if parent == -1 else i * 10 + parent
            parentId = ''
            parentsCount = random.randint(0, j+20)
            if parentsCount == 0 : parentId = '-1'
            for np in range(parentsCount):
                ids = SubTasks.loc[SubTasks['taskID'] == i]['SubTaskID'].sample().item()
                #ids = random.choice(SubTasks.loc[SubTasks['taskID'] == i]['SubTaskID'])
                parentId += str(ids) + ','

            row = {"taskID": i, "SubTaskID": i * 10 + j, "duration": duration, "parentId": parentId, 'Li': -1}

            r = pd.DataFrame(row , index=[0])

            SumDurationRow = 0
            while True:
                r2 = r.iloc[0]
                l=-2
                for ri in range(len(r)):
                    try:
                        if l < int(r.loc[ri]['Li']):
                            l = int(r.loc[ri]['Li'])
                            r2 =dict(r.loc[ri])
                    except:
                        pass
                r = r2

                print(r)
                p = r['parentId'].split(',')
                print(p)
                p = list(set([int(x) for x in p if x != ""]))

                SumDurationRow = SumDurationRow + int(r['duration'])
                if len(p) == 1 and int(p[0]) == -1:
                    row['Li'] = int(SumDurationRow)
                    break

                r = SubTasks.loc[SubTasks['SubTaskID'].isin(p)]

            SubTasks = SubTasks._append(row, ignore_index=True)

            # for l in range(i*10,i*10+j):
            #     SubTasks.loc[SubTasks['SubTaskID'] == l]

    tasks = pd.DataFrame(columns=['TaskID', "c", "L", "T", "D"])
    for i in range(dataSize):
        lst = SubTasks.loc[SubTasks['taskID'] == i]
        Ci = int(sum(lst.duration))
        Li = int(max(lst.Li))
        Ti = Di = int(random.randint(Li, Li + 2000))
        Ui = Ci / Di
        Ni = int(len(lst))
        row = {'TaskID': int(i), "c": Ci, 'L': Li, 'T': Ti, 'D': Di, 'U': Ui, 'N': Ni}
        tasks = tasks._append(row, ignore_index=True)

    return tasks, SubTasks


def Decomposition_DAG(task, subTasks):
    # M = threads number in segment
    # s = Duration of Segment

    segments = pd.DataFrame(columns=["taskID", "M", "S", "start", 'end'])
    for i in range(task):
        list = subTasks.loc[subTasks['taskID'] == i]
        for j in range(list):
            parents = list.loc[list['parentId'] == -1]
            # for
            # break

    return


def exe_DAGFluid(task, subTask):
    segments = Decomposition_DAG(task, subTask)
    return


tasks, subTasks = create_tasks(100)

print(tasks, "\n", subTasks)
