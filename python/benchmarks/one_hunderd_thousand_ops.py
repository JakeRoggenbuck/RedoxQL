from lstore.db import Database
from lstore.query import Query
import time
import statistics

iters = 30
times = []

for x in range(iters):
    start = time.time()

    amount = 100_000
    db = Database()

    grades_table = db.create_table("Grades", 5, 0)
    query = Query(grades_table)

    for i in range(0, amount):
        query.insert(10 + i, 93, 0, 0, 0)

    for i in range(0, amount):
        query.update(10 + i, *[10 + i, 100, 10, 20, 30])

    for i in range(0, amount):
        v = query.select(10 + i, 0, [1, 1, 1, 1, 1])[0]

    delta = time.time() - start
    times.append(delta)
    print("Time taken: ", delta)


print("Mean: ", statistics.mean(times))
print("Stand. Dev: ", statistics.stdev(times))

"""
# Results from removing if statements in the Python functions
# Possibly not significant

(venv) benchmarks (opt) λ p one_hunderd_thousand_ops.py
Time taken:  0.3500523567199707
Time taken:  0.3941800594329834
Time taken:  0.39363694190979004
Time taken:  0.391937255859375
Time taken:  0.38867855072021484
Time taken:  0.3892066478729248
Time taken:  0.3898959159851074
Time taken:  0.3931922912597656
Time taken:  0.39282822608947754
Time taken:  0.38979125022888184
Time taken:  0.3893275260925293
Time taken:  0.39595937728881836
Time taken:  0.39574217796325684
Time taken:  0.39283275604248047
Time taken:  0.39379262924194336
Time taken:  0.397397518157959
Time taken:  0.3945751190185547
Time taken:  0.38927292823791504
Time taken:  0.39089393615722656
Time taken:  0.39059996604919434
Time taken:  0.3915126323699951
Time taken:  0.39020848274230957
Time taken:  0.39324140548706055
Time taken:  0.38915491104125977
Time taken:  0.39886999130249023
Time taken:  0.3935694694519043
Time taken:  0.3947749137878418
Time taken:  0.3949851989746094
Time taken:  0.39185523986816406
Time taken:  0.3930368423461914
Mean:  0.3911667505900065
Stand. Dev:  0.008187510854573659
(venv) benchmarks (opt) λ p one_hunderd_thousand_ops.py
Time taken:  0.3379385471343994
Time taken:  0.3808896541595459
Time taken:  0.3768312931060791
Time taken:  0.3802781105041504
Time taken:  0.3807034492492676
Time taken:  0.3790574073791504
Time taken:  0.38037919998168945
Time taken:  0.3789088726043701
Time taken:  0.38813161849975586
Time taken:  0.3801705837249756
Time taken:  0.37989020347595215
Time taken:  0.38227391242980957
Time taken:  0.3826777935028076
Time taken:  0.38353800773620605
Time taken:  0.38106536865234375
Time taken:  0.38124990463256836
Time taken:  0.4272141456604004
Time taken:  0.383831262588501
Time taken:  0.3824472427368164
Time taken:  0.38194727897644043
Time taken:  0.38509297370910645
Time taken:  0.3860294818878174
Time taken:  0.38332533836364746
Time taken:  0.38790369033813477
Time taken:  0.3829309940338135
Time taken:  0.388042688369751
Time taken:  0.38141942024230957
Time taken:  0.3748810291290283
Time taken:  0.3886587619781494
Time taken:  0.38782620429992676
Mean:  0.38251781463623047
Stand. Dev:  0.012195962820217126
"""
