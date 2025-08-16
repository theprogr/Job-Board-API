from fastapi import FastAPI, status, Response, HTTPException
from pydantic import BaseModel
from typing import Optional
import psycopg
from psycopg.rows import dict_row
import time

app = FastAPI()

class Job(BaseModel):
    title: str
    description: str
    company: str
    location: Optional[str] = "Almaty"
    salary: Optional[int] = 150000
    is_active: bool = True


while True:
    try:
        conn = psycopg.connect(host="fake", dbname="fake", user="fake", password="fake", row_factory=dict_row)
        cursor = conn.cursor()
        print("Database connection was successfull!")
        break
    except Exception as error:
        print("Connecting to database failed")
        print("Errord: ", error)
        time.sleep(2)


@app.get("/jobs")
def view_all_jobs():
    cursor.execute("""SELECT * FROM jobs""")
    jobs = cursor.fetchall()
    return {"Jobs": jobs}


@app.get("/jobs/is_active")
def active_jobs():
    cursor.execute("""SELECT * FROM jobs WHERE is_active = true""")
    active_jobs = cursor.fetchall()
    return {"Jobs": active_jobs}


@app.get("/jobs/{id}")
def view_job(id: int):
    cursor.execute("""SELECT * FROM jobs WHERE id = %s""", (id,))
    job = cursor.fetchone()

    if job is None:
        raise HTTPException(status_code=404, detail=f"Job with an id of {id} was not found!")
    return {"Job": job}


@app.post("/jobs", status_code=status.HTTP_201_CREATED)
def new_job(job: Job):
    cursor.execute("""
        INSERT INTO jobs (title, description, company, location, salary)
        VALUES (%s, %s, %s, %s, %s) RETURNING *
    """, (job.title, job.description, job.company, job.location, job.salary))

    new_job = cursor.fetchone()
    conn.commit()
    return {"new job": new_job}


@app.put("/jobs/{id}")
def update_job(id: int, job: Job):
    cursor.execute("""UPDATE jobs SET company = %s, salary = %s 
                   WHERE id = %s RETURNING *""", (job.company, job.salary, id))
    updated_job = cursor.fetchone()
    conn.commit()

    if updated_job is None:
        raise HTTPException(status_code=404, detail=f"Job with an id of {id} was not found!")
    return {"Job": updated_job}


@app.delete("/jobs/{id}")
def delete_job(id: int):
    cursor.execute("DELETE FROM jobs WHERE id = %s RETURNING *", (id,))
    deleted_job = cursor.fetchone()
    conn.commit()

    if deleted_job is None:
        raise HTTPException(status_code=404, detail=f"Job with an id of {id} was not found!")
    return Response(status_code=status.HTTP_204_NO_CONTENT)
