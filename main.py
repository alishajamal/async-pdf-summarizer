from fastapi import FastAPI, HTTPException, UploadFile, File, status, BackgroundTasks
import asyncio, uuid, os
from dotenv import load_dotenv
from fastapi.responses import StreamingResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from google import genai
from arq import create_pool
from arq.connections import RedisSettings
import json
import asyncpg
from contextlib import asynccontextmanager

load_dotenv()
client = genai.Client(api_key=os.getenv("LLM_API_KEY"))
summary_semaphore = asyncio.Semaphore(2)
DB_URL = os.getenv("DATABASE_URL")

@asynccontextmanager
async def lifespan(app: FastAPI):
    redis_host = os.getenv("REDIS_HOST", "localhost")
    app.state.redis = await create_pool(RedisSettings(host=redis_host))
    
    if DB_URL:
        app.state.db_pool = await asyncpg.create_pool(DB_URL)
        async with app.state.db_pool.acquire() as con:
            await con.execute("""
                CREATE TABLE IF NOT EXISTS jobs(
                    id TEXT PRIMARY KEY, 
                    document_name TEXT, 
                    status TEXT, 
                    extracted_text TEXT, 
                    error_detail TEXT,
                    file_data BYTEA
                );
            """)
            
    yield  
    
    if hasattr(app.state, "redis"):
        app.state.redis.close()
        await app.state.redis.wait_closed()
    if hasattr(app.state, "db_pool"):
        await app.state.db_pool.close()

app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def serve_frontend():
    return FileResponse("static/index.html")

@app.post("/upload", status_code=status.HTTP_202_ACCEPTED)
async def upload(file: UploadFile = File(...)):
    job_id = uuid.uuid4().hex
    
    file_bytes = await file.read()
    
    async with app.state.db_pool.acquire() as con:
        await con.execute(
            "INSERT INTO jobs(id, document_name, status, file_data) VALUES ($1, $2, $3, $4)",  
            job_id, file.filename, "pending", file_bytes
        )
    
    await app.state.redis.enqueue_job("extract_pdf_text", job_id)
    
    return {"job_id": job_id}

@app.get("/jobs/{job_id}")
async def get_jobs(job_id: str):
    async with app.state.db_pool.acquire() as con:
        row = await con.fetchrow(
            "SELECT id, document_name, status, extracted_text, error_detail FROM jobs WHERE id = $1", 
            job_id
        )
    
    if row:
        return {
            "id": row['id'],
            "document_name": row['document_name'],
            "status": row['status'],
            "extracted_text": row['extracted_text'],
            "error_detail": row['error_detail']
        }
    raise HTTPException(status_code=404, detail="Job not found")

async def stream_summary(text: str):
    try:
        async with summary_semaphore:
            response = await client.aio.models.generate_content_stream(
                model='gemini-3.6-flash',
                contents=f"Provide a concise, professional summary of the following document and Format the summary using standard Markdown. DO NOT use LaTeX, mathematical formatting, or $ symbols (e.g., use '->' instead of '\\rightarrow'):\n\n{text}"
            )
            async for chunk in response:
                if chunk.text:
                    payload = json.dumps({"text": chunk.text})
                    yield f"data: {payload}\n\n"
    except Exception as e:
        print(f"error is {e}")
        yield f"data: ERROR -{str(e)}\n\n"

@app.get("/jobs/{job_id}/summary")
async def get_summary(job_id: str):
    async with app.state.db_pool.acquire() as con:
        row = await con.fetchrow("SELECT status, extracted_text FROM jobs WHERE id = $1", job_id)
    
    if not row:
        raise HTTPException(status_code=404, detail="job not found")
    
    status_val = row['status']
    extracted_text = row['extracted_text']
    
    if status_val == "failed":
        raise HTTPException(status_code=400, detail="the doc failed to be parsed")
    if status_val in ["pending", "processing"]:
        raise HTTPException(status_code=400, detail="document isnt ready to show")
    
    return StreamingResponse(
        stream_summary(extracted_text), media_type="text/event-stream"
    )