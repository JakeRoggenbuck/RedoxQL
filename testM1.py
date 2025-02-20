from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

score = 0
def speed_tester1():
    print("Checking exam M1 normal tester");
    global score
    db = Database()
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

    number_of_records = 1000
    number_of_aggregates = 100
    seed(3562901)

    for i in range(0, number_of_records):
        key = 92106429 + randint(0, number_of_records)

        #skip duplicate keys
        while key in records:
            key = 92106429 + randint(0, number_of_records)

        records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
        query.insert(*records[key])
    print("Insert finished")

    # Check inserted records using select query
    for key in records:
        # select function will return array of records 
        # here we are sure that there is only one record in that array
        record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
        error = False
        for i, column in enumerate(record.columns):
            if column != records[key][i]:
                error = True
        if error:
            raise Exception('select error on', key, ':', record.columns, ', correct:', records[key])
        else:
            pass
    
    
    updated_records = {}
    for key in records:
        updated_columns = [None, None, None, None, None]
        updated_records[key] = records[key].copy()
        for i in range(2, grades_table.num_columns):
            # updated value
            value = randint(0, 20)
            updated_columns[i] = value
            # update our test directory
            updated_records[key][i] = value
        query.update(key, *updated_columns)

        #check updated result for record
        record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
        error = False
        for j, column in enumerate(record.columns):
            if column != updated_records[key][j]:
                error = True
        if error:
            print('update error on', records[key], 'and', updated_columns, ':', record.columns, ', correct:', records[key])
        else:
            pass
    score = score + 15
    
    keys = sorted(list(records.keys()))
    # aggregate on every column 
    for c in range(0, grades_table.num_columns):
        for i in range(0, number_of_aggregates):
            r = sorted(sample(range(0, len(keys)), 2))
            # calculate the sum form test directory
            # version 0 sum
            updated_column_sum = sum(map(lambda key: updated_records[key][c], keys[r[0]: r[1] + 1]))
            updated_result = query.sum(keys[r[0]], keys[r[1]], c)
            if updated_column_sum != updated_result:
                raise Exception('sum error on column', c, '[', keys[r[0]], ',', keys[r[1]], ']: ', updated_result, ', correct: ', updated_column_sum)
            else:
                pass
    score = score + 15


def speed_tester2():
    print("\n\nChecking exam M1 extended tester");
    global score
    db = Database()
    # Create a table  with 5 columns
    #   Student Id and 4 grades
    #   The first argument is name of the table
    #   The second argument is the number of columns
    #   The third argument is determining the which columns will be primary key
    #       Here the first column would be student id and primary key
    grades_table = db.create_table('Grades', 5, 0)

    # create a query class for the grades table
    query = Query(grades_table)

    # dictionary for records to test the database: test directory
    records = {}

    number_of_records = 1000
    number_of_aggregates = 100
    number_of_updates = 5
    seed(3562901)

    for i in range(0, number_of_records):
        key = 92106429 + randint(0, number_of_records)

        #skip duplicate keys
        while key in records:
            key = 92106429 + randint(0, number_of_records)

        records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
        query.insert(*records[key])
        # print('inserted', records[key])
    print("Insert finished")

    # Check inserted records using select query
    for key in records:
        # select function will return array of records 
        # here we are sure that there is only one record in that array
        record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
        error = False
        for i, column in enumerate(record.columns):
            if column != records[key][i]:
                error = True
        if error:
            raise Exception('select error on', key, ':', record.columns, ', correct:', records[key])
        else:
            pass
            # print('select on', key, ':', record)
    
    
    all_updates = []
    keys = sorted(list(records.keys()))
    for i in range(number_of_updates):
        all_updates.append({})
        for key in records:
            updated_columns = [None, None, None, None, None]
            all_updates[i][key] = records[key].copy()
            for j in range(2, grades_table.num_columns):
                # updated value
                value = randint(0, 20)
                updated_columns[j] = value
                # update our test directory
                all_updates[i][key][j] = value
            query.update(key, *updated_columns)
    
    try:
        # Check records that were persisted in part 1
        version = 0
        expected_update = records if version <= -number_of_updates else all_updates[version + number_of_updates - 1]
        for key in keys:
            record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
            error = False
            for k, column in enumerate(record.columns):
                if column != expected_update[key][k]:
                    error = True
            if error:
                raise Exception('select error on', key, ':', record.columns, ', correct:', expected_update[key])
                break
        print("Select version ", version, "finished")
        score = score + 20
    except Exception as e:
        score = score + 5
        print("Something went wrong during select")
        print(e)
    
    try:
        version = 0
        expected_update = records if version <= -number_of_updates else all_updates[version + number_of_updates - 1]
        for j in range(0, number_of_aggregates):
            r = sorted(sample(range(0, len(keys)), 2))
            column_sum = sum(map(lambda x: expected_update[x][0] if x in expected_update else 0, keys[r[0]: r[1] + 1]))
            result = query.sum(keys[r[0]], keys[r[1]], 0)
            if column_sum != result:
                raise Exception('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
        print("Aggregate version ", version, "finished")
        score = score + 20
    except Exception as e:
        score = score + 5
        print("Something went wrong during sum")
        print(e)
    db.close()

def correctness_tester():
    global score
    db = Database()
    grades_table = db.create_table('Grades', 5, 0)

    # create a query class for the grades table
    query = Query(grades_table)

    # dictionary for records to test the database: test directory
    records = {}
    records[1] = [1, 1, 1, 1, 1]
    records[2] = [1, 2, 2, 2, 2]
    records[3] = [2, 3, 3, 3, 3]
    records[4] = [1, 2, 2, 2, 2]
    query.insert(*records[1])
    # Test if correct columns are returned 
    result = query.select(1, 0, [1,0,1,0,0])
    print(len(result))
    print(result[0].columns)
    if len(result) == 1 and len(result[0].columns) == 2 and result[0].columns[1] == records[1][2]:
        score += 5
        print("[0] pass")
    elif len(result) == 1 and result[0].columns[0] == 1 and result[0].columns[2] == 1 and\
        result[0].columns[3] == None and result[0].columns[4] == None and result[0].columns[1] == None:
        score += 5
        print("[0] pass")
    # Test if insertion with existing primary_key is not allowed
    query.insert(*records[2])
    result = query.select(1, 0, [1,1,1,1,1])
    print(len(result))
    print(0, result[0].columns, records[1])
    if len(result) == 1 and len(result[0].columns) == 5 and result[0].columns[1] == records[1][1]\
        and result[0].columns[2] == records[1][2] and result[0].columns[3] == records[1][3]\
        and result[0].columns[4] == records[1][4]:
        score += 5
        print("[1] pass")
    result = query.sum(1, 1, 1)
    if result == 1:
        score += 5
        print("[2] pass")
    # Test if updated record with existing primary_key is not allowed
    query.insert(*records[3])
    query.update(2, *records[4])
    try:
        result = query.select(1, 0, [1,0,1,0,0])
        print(1, result[0].columns, records[1])
        if len(result) == 1 and len(result[0].columns) == 2 and result[0].columns[1] == records[1][2]:
            score += 5
            print("[3] pass")
        elif len(result) == 1 and result[0].columns[0] == 1 and result[0].columns[2] == 1 and\
        result[0].columns[3] == None and result[0].columns[4] == None and result[0].columns[1] == None:
            score += 5
            print("[3] pass")
    except Exception as e:
        print("Something went wrong during update")
        print(e)
    result = query.select(2, 0, [1,1,1,1,1])
    print(len(result))
    if len(result) != 0:
        print(2, result[0].columns, records[3])
    if len(result) == 1 and len(result[0].columns) == 5 and result[0].columns[1] == records[3][1]\
        and result[0].columns[2] == records[3][2] and result[0].columns[3] == records[3][3]\
        and result[0].columns[4] == records[3][4]:
        score += 5
        print("[4] pass")

from timeit import default_timer as timer
from decimal import Decimal

import os
import glob
import traceback
import shutil   
    
def run_test():
    start = timer()
    try:
        speed_tester1()
    except Exception as e:
        print("Something went wrong")
        print(e)
        traceback.print_exc()

    try:
        speed_tester2()
    except Exception as e:
        print("Something went wrong")
        print(e)
        traceback.print_exc()

    end = timer()
    print("\n------------------------------------")
    print("Time taken: ", Decimal(end - start).quantize(Decimal('0.01')), "seconds")
    print("Total score for speed testers: ", score)
    print("--------------------------------------\n")

    try:
        correctness_tester()
    except Exception as e:
        print("Something went wrong")
        print(e)
        traceback.print_exc()
    print("Total score: ", score + 5)

run_test()
