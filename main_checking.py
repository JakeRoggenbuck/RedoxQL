from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange
from lstore import print_logo

print()
print_logo()

def wrap_green(text):
    text = str(text)

    bold = "\033[1m"
    green = "\033[32m"
    reset = "\033[0m"

    full_text = bold

    still_zero = True

    for c in text:
        if c not in set("0.") and still_zero:
            still_zero = False
            full_text += green

        full_text += c

    return full_text + reset

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []

insert_time_0 = process_time()
for i in range(0, 10000):
    a = query.insert(906659671 + i, 93, 0, 0, 0)

    # Print occasionally
    # if i % 100 == 0:
    #     print(a)

    keys.append(906659671 + i)
insert_time_1 = process_time()

print("Inserting 10k records took:  \t\t\t", wrap_green(insert_time_1 - insert_time_0))

# Measuring update Performance
update_cols = [
    [None, None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]

update_time_0 = process_time()
for i in range(0, 10000):
    query.update(choice(keys), *(choice(update_cols)))
update_time_1 = process_time()
print("Updating 10k records took:  \t\t\t", wrap_green(update_time_1 - update_time_0))

# Measuring Select Performance
select_time_0 = process_time()
for i in range(0, 10000):
    a = query.select(choice(keys),0 , [1, 1, 1, 1, 1])

    # Print occasionally
    # if i % 100 == 0:
    #     print(a)

select_time_1 = process_time()
print("Selecting 10k records took:  \t\t\t", wrap_green(select_time_1 - select_time_0))

# Measuring Aggregate Performance
agg_time_0 = process_time()
for i in range(0, 10000, 100):
    start_value = 906659671 + i
    end_value = start_value + 100
    result = query.sum(start_value, end_value - 1, randrange(0, 5))
agg_time_1 = process_time()
print("Aggregate 10k of 100 record batch took:\t\t", wrap_green(agg_time_1 - agg_time_0))

# Measuring Delete Performance
delete_time_0 = process_time()
for i in range(0, 10000):
    query.delete(906659671 + i)
delete_time_1 = process_time()
print("Deleting 10k records took:  \t\t\t", wrap_green(delete_time_1 - delete_time_0))

print()
