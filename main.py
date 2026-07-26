from fastapi import FastAPI, HTTPException, UploadFile, File, status, BackgroundTasks
from pydantic import BaseModel
import aiofiles, asyncio, uuid, sqlite3, os
from dotenv import load_dotenv
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from google import genai
from arq import create_pool
from arq.connections import RedisSettings
import json

load_dotenv()
client = genai.Client(api_key=os.getenv("LLM_API_KEY"))
jobs_db = {}

summary_semaphore = asyncio.Semaphore(2)
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")
@app.get("/")
async def serve_frontend():
    return FileResponse("static/index.html")


def init_db():
    con = sqlite3.connect("jobs.db")
    cur = con.cursor()
    statement = """CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY,document_name TEXT, status TEXT, extracted_text TEXT, error_detail TEXT);"""
    cur.execute(statement)
    cur.close()
    cur.close()

init_db()

class JobCreate(BaseModel):
    document_name : str
    file_size_kb : int



@app.post("/upload", status_code=status.HTTP_202_ACCEPTED)
async def upload(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    job_id = uuid.uuid4().hex
    con = sqlite3.connect("jobs.db")
    cur = con.cursor()
    cur.execute("INSERT INTO jobs(id, document_name, status) VALUES (?, ?, ?)",  (job_id, file.filename, "pending"))
    con.commit()
    CHUNK_SIZE = 1024 * 1024
    async with aiofiles.open(file.filename, "wb") as f:
        while chunk := await file.read(CHUNK_SIZE):
            await f.write(chunk)
    redis = await create_pool(RedisSettings())
    await redis.enqueue_job("extract_pdf_text", job_id, file.filename)
    con.close()
    return {"job_id": job_id}

@app.get("/jobs/{job_id}")
def get_jobs(job_id: str):
    con = sqlite3.connect("jobs.db")
    cur = con.cursor()
    cur.execute("SELECT * FROM jobs WHERE id = ?", (job_id,))
    row = cur.fetchone()
    con.close()
    if row:
        return {
            "id": row[0],
            "document_name": row[1],
            "status": row[2],
            "extracted_text": row[3],
            "error_detail": row[4]
        }
    else:
        raise HTTPException(status_code=404, detail="job isnt available")

async def stream_summary(text: str):
    try:
        async with summary_semaphore:
            response = await client.aio.models.generate_content_stream(
                model = 'gemini-3.6-flash',
                contents = f"Provide a concise, professional summary of the following document and Format the summary using standard Markdown. DO NOT use LaTeX, mathematical formatting, or $ symbols (e.g., use '->' instead of '\\rightarrow'):\n\n{text}"
            )
            async for chunk in response:
                if chunk.text:
                    payload = json.dumps({"text": chunk.text})
                    yield f"data: {payload}\n\n"
    except Exception as e:
        print(F"error is {e}")
        yield f"data: ERROR -{str(e)}\n\n"

@app.get("/jobs/{job_id}/summary")
def get_summary(job_id: str):
    con = sqlite3.connect("jobs.db")
    cur = con.cursor()
    cur.execute("SELECT status, extracted_text FROM jobs WHERE id = ?", (job_id,))
    row = cur.fetchone()
    con.close()
    if not row:
        raise HTTPException(status_code=404, detail="job not found")
    status = row[0]
    extracted_text = row[1]
    if status == "failed":
        raise HTTPException(status_code=400, detail="the doc failed to be parsed")
    if status == "pending" or status == "processing":
        raise HTTPException(status_code=400, detail="document isnt rdy to show")
    
    return StreamingResponse(
        stream_summary(extracted_text), media_type = "text/event-stream"
    )



