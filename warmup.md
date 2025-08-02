## Adding an insert warmup

We could have shaved off 0.839ms (or made it 3.58x faster!) simply by giving the database a warmup run before the benchmarks run.

>>> without = [0.0117210, 0.0117610, 0.0113612, 0.0118570, 0.0114723]
>>> with_warmup = [0.0032362, 0.0033220, 0.0031920, 0.0032789, 0.0032101]

I discovered this by accident comparing the `./main_checking.py` and `./__main__.py` runtimes, which seem to be different.

## Data

### Without Warmup

```
(venv) redoxql-m3 (main) λ p main_checking.py


    ______         _           _____ _
    | ___ \       | |         |  _  | |
    | |_/ /___  __| | _____  _| | | | |
    |    // _ \/ _` |/ _ \ \/ / | | | |
    | |\ \  __/ (_| | (_) >  <\ \/' / |____
    \_| \_\___|\__,_|\___/_/\_\\_/\_\_____/

    =======================================

    RedoxQL is an L-Store database written
    in Rust and Python.

    Milestone: 1

Inserting 10k records took:  			 0.0117210 seconds
Insert 10k records average:			 0.0059220 seconds
Updating 10k records took:  			 0.0217667 seconds
Selecting 10k records took:  			 0.0152404 seconds
Aggregate 10k of 100 record batch took:		 0.0065665 seconds
Deleting 10k records took:  			 0.0032334 seconds

(venv) redoxql-m3 (main) λ p main_checking.py


    ______         _           _____ _
    | ___ \       | |         |  _  | |
    | |_/ /___  __| | _____  _| | | | |
    |    // _ \/ _` |/ _ \ \/ / | | | |
    | |\ \  __/ (_| | (_) >  <\ \/' / |____
    \_| \_\___|\__,_|\___/_/\_\\_/\_\_____/

    =======================================

    RedoxQL is an L-Store database written
    in Rust and Python.

    Milestone: 1

Inserting 10k records took:  			 0.0117610 seconds
Insert 10k records average:			 0.0060219 seconds
Updating 10k records took:  			 0.0216311 seconds
Selecting 10k records took:  			 0.0153234 seconds
Aggregate 10k of 100 record batch took:		 0.0061702 seconds
Deleting 10k records took:  			 0.0037029 seconds

(venv) redoxql-m3 (main) λ p main_checking.py


    ______         _           _____ _
    | ___ \       | |         |  _  | |
    | |_/ /___  __| | _____  _| | | | |
    |    // _ \/ _` |/ _ \ \/ / | | | |
    | |\ \  __/ (_| | (_) >  <\ \/' / |____
    \_| \_\___|\__,_|\___/_/\_\\_/\_\_____/

    =======================================

    RedoxQL is an L-Store database written
    in Rust and Python.

    Milestone: 1

Inserting 10k records took:  			 0.0113612 seconds
Insert 10k records average:			 0.0059399 seconds
Updating 10k records took:  			 0.0216389 seconds
Selecting 10k records took:  			 0.0150633 seconds
Aggregate 10k of 100 record batch took:		 0.0085654 seconds
Deleting 10k records took:  			 0.0049268 seconds

(venv) redoxql-m3 (main) λ p main_checking.py


    ______         _           _____ _
    | ___ \       | |         |  _  | |
    | |_/ /___  __| | _____  _| | | | |
    |    // _ \/ _` |/ _ \ \/ / | | | |
    | |\ \  __/ (_| | (_) >  <\ \/' / |____
    \_| \_\___|\__,_|\___/_/\_\\_/\_\_____/

    =======================================

    RedoxQL is an L-Store database written
    in Rust and Python.

    Milestone: 1

Inserting 10k records took:  			 0.0118570 seconds
Insert 10k records average:			 0.0060450 seconds
Updating 10k records took:  			 0.0212251 seconds
Selecting 10k records took:  			 0.0149994 seconds
Aggregate 10k of 100 record batch took:		 0.0068376 seconds
Deleting 10k records took:  			 0.0036636 seconds

(venv) redoxql-m3 (main) λ p main_checking.py


    ______         _           _____ _
    | ___ \       | |         |  _  | |
    | |_/ /___  __| | _____  _| | | | |
    |    // _ \/ _` |/ _ \ \/ / | | | |
    | |\ \  __/ (_| | (_) >  <\ \/' / |____
    \_| \_\___|\__,_|\___/_/\_\\_/\_\_____/

    =======================================

    RedoxQL is an L-Store database written
    in Rust and Python.

    Milestone: 1

Inserting 10k records took:  			 0.0114723 seconds
Insert 10k records average:			 0.0060868 seconds
Updating 10k records took:  			 0.0214978 seconds
Selecting 10k records took:  			 0.0151961 seconds
Aggregate 10k of 100 record batch took:		 0.0072582 seconds
Deleting 10k records took:  			 0.0041888 seconds
```

### With Warmup

```
(venv) redoxql-m3 (main) λ p main_checking.py


    ______         _           _____ _
    | ___ \       | |         |  _  | |
    | |_/ /___  __| | _____  _| | | | |
    |    // _ \/ _` |/ _ \ \/ / | | | |
    | |\ \  __/ (_| | (_) >  <\ \/' / |____
    \_| \_\___|\__,_|\___/_/\_\\_/\_\_____/

    =======================================

    RedoxQL is an L-Store database written
    in Rust and Python.

    Milestone: 1

Inserting 10k records took:  			 0.0032362 seconds
Insert 10k records average:			 0.0059782 seconds
Updating 10k records took:  			 0.0218384 seconds
Selecting 10k records took:  			 0.0160259 seconds
Aggregate 10k of 100 record batch took:		 0.0067736 seconds
Deleting 10k records took:  			 0.0035029 seconds

(venv) redoxql-m3 (main) λ p main_checking.py


    ______         _           _____ _
    | ___ \       | |         |  _  | |
    | |_/ /___  __| | _____  _| | | | |
    |    // _ \/ _` |/ _ \ \/ / | | | |
    | |\ \  __/ (_| | (_) >  <\ \/' / |____
    \_| \_\___|\__,_|\___/_/\_\\_/\_\_____/

    =======================================

    RedoxQL is an L-Store database written
    in Rust and Python.

    Milestone: 1

Inserting 10k records took:  			 0.0033220 seconds
Insert 10k records average:			 0.0060065 seconds
Updating 10k records took:  			 0.0224251 seconds
Selecting 10k records took:  			 0.0185516 seconds
Aggregate 10k of 100 record batch took:		 0.0096829 seconds
Deleting 10k records took:  			 0.0036378 seconds

(venv) redoxql-m3 (main) λ p main_checking.py


    ______         _           _____ _
    | ___ \       | |         |  _  | |
    | |_/ /___  __| | _____  _| | | | |
    |    // _ \/ _` |/ _ \ \/ / | | | |
    | |\ \  __/ (_| | (_) >  <\ \/' / |____
    \_| \_\___|\__,_|\___/_/\_\\_/\_\_____/

    =======================================

    RedoxQL is an L-Store database written
    in Rust and Python.

    Milestone: 1

Inserting 10k records took:  			 0.0031920 seconds
Insert 10k records average:			 0.0060142 seconds
Updating 10k records took:  			 0.0215623 seconds
Selecting 10k records took:  			 0.0155171 seconds
Aggregate 10k of 100 record batch took:		 0.0064726 seconds
Deleting 10k records took:  			 0.0033066 seconds

(venv) redoxql-m3 (main) λ p main_checking.py


    ______         _           _____ _
    | ___ \       | |         |  _  | |
    | |_/ /___  __| | _____  _| | | | |
    |    // _ \/ _` |/ _ \ \/ / | | | |
    | |\ \  __/ (_| | (_) >  <\ \/' / |____
    \_| \_\___|\__,_|\___/_/\_\\_/\_\_____/

    =======================================

    RedoxQL is an L-Store database written
    in Rust and Python.

    Milestone: 1

Inserting 10k records took:  			 0.0032789 seconds
Insert 10k records average:			 0.0060473 seconds
Updating 10k records took:  			 0.0218113 seconds
Selecting 10k records took:  			 0.0153821 seconds
Aggregate 10k of 100 record batch took:		 0.0062161 seconds
Deleting 10k records took:  			 0.0031495 seconds

(venv) redoxql-m3 (main) λ p main_checking.py


    ______         _           _____ _
    | ___ \       | |         |  _  | |
    | |_/ /___  __| | _____  _| | | | |
    |    // _ \/ _` |/ _ \ \/ / | | | |
    | |\ \  __/ (_| | (_) >  <\ \/' / |____
    \_| \_\___|\__,_|\___/_/\_\\_/\_\_____/

    =======================================

    RedoxQL is an L-Store database written
    in Rust and Python.

    Milestone: 1

Inserting 10k records took:  			 0.0032101 seconds
Insert 10k records average:			 0.0062221 seconds
Updating 10k records took:  			 0.0240348 seconds
Selecting 10k records took:  			 0.0174595 seconds
Aggregate 10k of 100 record batch took:		 0.0067344 seconds
Deleting 10k records took:  			 0.0033091 seconds
```
