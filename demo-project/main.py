from fastapi import FastAPI
from lstore.db import Database
from lstore.query import Query
from pydantic import BaseModel


app = FastAPI()
db = Database()
scores_table = db.create_table('Scores', 5, 0)
query = Query(scores_table)


class Scores(BaseModel):
    student_id: int
    score_1: int
    score_2: int
    score_3: int
    score_4: int


"""
Here is how you can access a score

```sh
curl "http://127.0.0.1:8000/values/1"
```
"""
@app.get("/values/{value_id}")
def read_value(value_id: int):
    res = query.select(value_id, 0, [1] * 5)

    if len(res) > 0:
        val = res[0]

        if val is not None:
            return val.columns


"""
Here is how you can add data using curl.

```sh
curl -X PUT "http://127.0.0.1:8000/values"
    -H "Content-Type: application/json"
    -d '{
        "student_id": 100200300,
        "score_1": 94,
        "score_2": 91,
        "score_3": 98,
        "score_4": 94
    }'
```
"""
@app.put("/values")
def update_item(values: Scores):
    return query.insert(
        values.student_id,
        values.score_1,
        values.score_2,
        values.score_3,
        values.score_4,
    )
