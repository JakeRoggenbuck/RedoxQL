from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed


records = {}
number_of_records = 1000
number_of_aggregates = 100
number_of_updates = 1
keys = {}

def reorganize_result(result):
    val = list()
    for r in result:
        val.append(r.columns)
    val.sort()
    return val

# 30 points in total
def correctness_tester1():
    records = [
        [0, 1, 1, 2, 1],
        [1, 1, 1, 1, 2],
        [2, 0, 3, 5, 1],
        [3, 1, 5, 1, 3],
        [4, 2, 7, 1, 1],
        [5, 1, 1, 1, 1],
        [6, 0, 9, 1, 0],
        [7, 1, 1, 1, 1],
    ]
    db = Database()
    db.open("./CT")
    test_table = db.create_table('test', 5, 0)
    query = Query(test_table)
    for record in records:
        query.insert(*record)
    try:
        # select on columns with index
        test_table.index.create_index(2)
        result = reorganize_result(query.select(1, 2, [1,1,1,1,1]))
        if len(result) == 4:
            if records[0] in result and records[1] in result and records[5] in result and records[7] in result:
                print("PASS[0]")
            else:
                print("Error[0]")
        else:
            print("Error[0]")
    except Exception as e:
        print("Wrong[0]")

    try:
        # select on columns without index and return 1 record
        test_table.index.drop_index(2)
        result = reorganize_result(query.select(3, 2, [1,1,1,1,1]))
        if len(result) == 1 and records[2] in result:
            print("PASS[1]")
        else:
            print("Error[1]")
    except Exception as e:
        print("Wrong[1]")

    try:
    # select on columns without index and return multiple records
        result = reorganize_result(query.select(1, 2, [1,1,1,1,1]))
        if len(result) == 4:
            if records[0] in result and records[1] in result and records[5] in result and records[7] in result:
                print("PASS[2]")
            else:
                print("Error[2]")
        else:
            print("Error[2]")
    except Exception as e:
        print("Wrong[2]")

    try:
        # select on columns without index and return empty list
        result = reorganize_result(query.select(10, 2, [1,1,1,1,1]))
        print(result)
        if len(result) == 0:
            print("PASS[3]")
        else:
            print("Error[3]")
    except Exception as e:
        print("Wrong[3]")

    try:
        # update on a primary key that does not exits
        query.update(8, *[None,2,2,2,2])
        result = reorganize_result(query.select(8, 0, [1,1,1,1,1]))
        if len(result) == 0:
            print("PASS[4]")
        else:
            print("Error[4]")
    except Exception as e:
        print("Wrong[4]")

    try:
        # update that changes primary key,
        query.update(7, *[8,2,2,2,2])
        result = reorganize_result(query.select(7, 0, [1,1,1,1,1]))
        if len(result) == 0:
            print("PASS[5]")
        else:
            print("Error[5]")
    except Exception as e:
        print("Wrong[5]")

    try:
        # delete a record
        query.delete(5)
        result = reorganize_result(query.select(5, 0, [1,1,1,1,1]))
        if len(result) == 0:
            print("PASS[6]")
        else:
            print("Error[6]")
    except Exception as e:
        print("Wrong[6]")

    try:
        # multiple tables

        test_table2 = db.create_table("test2", 5, 0)
        records2 = [
            [1, 1, 1, 2, 1],
            [2, 1, 1, 1, 2],
            [3, 0, 3, 5, 1],
            [4, 1, 5, 1, 3],
            [5, 2, 7, 1, 1],
            [6, 1, 1, 1, 1],
            [7, 0, 9, 1, 0],
            [8, 1, 1, 1, 1],
        ]
        query2 = Query(test_table2)
        for record in records2:
            query2.insert(*record)
        result = reorganize_result(query2.select(1, 0, [1,1,1,1,1]))
        if len(result) == 1 and records2[0] in result:
            print("PASS[7]")
        else:
            print("Error[7]")
    except Exception as e:
        print("Wrong[7]")

def correctness_tester2():
    # different primary key
    try:
        db = Database()
        test_table3 = db.create_table("test3", 5, 2)
        records3 = [
            [1, 1, 0, 2, 1],
            [2, 1, 1, 1, 2],
            [3, 0, 2, 5, 1],
            [4, 1, 3, 1, 3],
            [5, 2, 4, 1, 1],
            [6, 1, 5, 1, 1],
            [7, 0, 6, 1, 0],
            [8, 1, 7, 1, 1],
        ]
        query3 = Query(test_table3)
        for record in records3:
            query3.insert(*record)
        result = query3.sum(3, 5, 4)
        print(result)
        if result == 5:
            print("PASS[8]")
        else:
            print("Error[8]")
    except Exception as e:
        print("Wrong[8]")


def generte_keys():
    global records, number_of_records, number_of_aggregates, number_of_updates, keys
    
    if True:
        records = {}
        seed(3562901)

        for i in range(0, number_of_records):
            key = 92106429 + i
            records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]

        keys = sorted(records.keys())
        
        for _ in range(number_of_updates):
            for key in keys:
                updated_columns = [None, None, None, None, None]
                # copy record to check
                for i in range(1, 5):
                    # updated value
                    value = randint(0, 20)
                    updated_columns[i] = value
                    # update our test directory
                    records[key][i] = value

def durability_tester1():
    print("Checking exam M2 durability")
    global records, number_of_records, number_of_aggregates, number_of_updates, keys
    
    if True:
        db = Database()
        db.open('./M2')
        # Create a table  with 5 columns
        #   Student Id and 4 grades
        #   The first argument is name of the table
        #   The second argument is the number of columns
        #   The third argument is determining the which columns will be primay key
        #       Here the first column would be student id and primary key
        grades_table = db.create_table('Grades', 5, 0)

        # create a query class for the grades table
        query = Query(grades_table)

        # dictionary for records to test the database: test directory
        records = {}

        seed(3562901)

        for i in range(0, number_of_records):
            key = 92106429 + i
            records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
            query.insert(*records[key])
        print("Insert finished")

        # Check inserted records using select query
        for key in keys:
            record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
            error = False
            for i, column in enumerate(record.columns):
                if column != records[key][i]:
                    error = True
            if error:
                print('select error on', key, ':', record, ', correct:', records[key])
            else:
                pass
                # print('select on', key, ':', record)
        print("Select finished")

        # x update on every column
        for _ in range(number_of_updates):
            for key in keys:
                updated_columns = [None, None, None, None, None]
                # copy record to check
                original = records[key].copy()
                for i in range(1, grades_table.num_columns):
                    # updated value
                    value = randint(0, 20)
                    updated_columns[i] = value
                    # update our test directory
                    records[key][i] = value
                query.update(key, *updated_columns)
                record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
                error = False
                for j, column in enumerate(record.columns):
                    if column != records[key][j]:
                        error = True
                if error:
                    raise Exception('update error on', original, 'and', updated_columns, ':', record.columns, ', correct:', records[key])
                else:
                    pass
                    # print('update on', original, 'and', updated_columns, ':', record)
        print("Update finished")

        for i in range(0, number_of_aggregates):
            r = sorted(sample(range(0, len(keys)), 2))
            column_sum = sum(map(lambda key: records[key][0], keys[r[0]: r[1] + 1]))
            result = query.sum(keys[r[0]], keys[r[1]], 0)
            if column_sum != result:
                print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
            else:
                pass
                # print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
        print("Aggregate finished")
        db.close()
        print("DB is closed")

def durability_tester2():
    # reopen the database
    global records, number_of_records, number_of_aggregates, number_of_updates, keys

    if True:
        db = Database()
        db.open('./M2')

        # Getting the existing Grades table
        grades_table = db.get_table('Grades')

        # create a query class for the grades table
        query = Query(grades_table)

        # dictionary for records to test the database: test directory

        # Check inserted records using select query
        err = False
        for key in keys:
            record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
            error = False
            for i, column in enumerate(record.columns):
                if column != records[key][i]:
                    error = True
            if error:
                err = True
                print('[Durability]select error on', key, ':', record.columns, ', correct:', records[key])
            else:
                pass
                # print('select on', key, ':', record)
        if not err:
            pass
        print("Select finished")
        
        err = False
        for i in range(0, number_of_aggregates):
            r = sorted(sample(range(0, len(keys)), 2))
            correct_result = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
            sum_result = query.sum(keys[r[0]], keys[r[1]], 0)
            if correct_result != sum_result:
                err = True
                raise Exception('[Durability]sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', sum_result, ', correct: ', correct_result)
            else:
                pass
        print("Aggregate finished")

        db.close()




def merging_tester():
    # Without Merging, the select would be extremely slow.
    db = Database()
    db.open("./MT")
    merge_table = db.create_table('merge', 5, 0)
    query = Query(merge_table)
    update_nums = [2, 4, 8, 16]
    records_num = 10000
    sample_count = 200
    select_repeat = 200
    for i in range(records_num):
        query.insert(*[i, (i+100)%records_num, (i+200)%records_num, (i+300)%records_num, (i+400)%records_num])
    for index in range(len(update_nums)):
        # 10000*4*(5+4*2+3*4+2*8+16*1) = 2280000 Byte = 556 Pages (4KB Page)
        update_num = update_nums[index]
        for count in range(update_num):
            for i in range(records_num):
                update_record = [None, (i+101+count)%records_num, (i+201+count)%records_num,\
                 (i+301+count)%records_num, (i+401+count)%records_num]
                for idx in range(index):
                    update_record[4-idx] = None
                query.update(i, *update_record)
        keys = sorted(sample(range(0, records_num),sample_count)) 
        time = 0
        # 200 * 200 select
        while time < select_repeat:
            time += 1
            for key in keys:
                query.select(key, 0, [1,1,1,1,1])

from timeit import default_timer as timer
from decimal import Decimal

import os
import glob
import traceback
import shutil   

m2tests = [1,1,1]
if m2tests[0] == 1:
    print("==========correctness tester===============")
    correctness_tester1() 
    correctness_tester2() 
if m2tests[1] == 1:
    print("==========durability tester================")
    generte_keys()
    durability_tester1()
    durability_tester2() 
if m2tests[2] == 1:
    print("==========merging tester===================")
    start = timer()
    merging_tester()
    end = timer()
    print()
    print("Total time Taken: ", Decimal(end - start).quantize(Decimal('0.01')), "seconds")


