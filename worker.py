import os
import fitz  # PyMuPDF
import asyncpg
from arq.connections import RedisSettings

async def extract_pdf_text(ctx, job_id: str):
    DB_URL = os.getenv("DATABASE_URL")
    
    con = await asyncpg.connect(DB_URL)
    
    try:
        await con.execute("UPDATE jobs SET status = 'processing' WHERE id = $1", job_id)

        row = await con.fetchrow("SELECT file_data FROM jobs WHERE id = $1", job_id)
        
        if not row or not row['file_data']:
            raise ValueError("File data not found in the database.")
            
        file_bytes = row['file_data']
        text = ""
        
        with fitz.open(stream=file_bytes, filetype="pdf") as doc:
            for page in doc:
                text += page.get_text()
                
        if not text.strip():
            raise ValueError("No selectable text found. Scanned documents are not supported.")
        
        await con.execute(
            """UPDATE jobs 
               SET status = 'completed', extracted_text = $1, file_data = NULL 
               WHERE id = $2""",
            text, job_id
        )
        print(f"Job {job_id} completed successfully.")
        
    except Exception as e:
        print(f"Job {job_id} failed: {str(e)}")
        await con.execute(
            "UPDATE jobs SET status = 'failed', error_detail = $1 WHERE id = $2", 
            str(e), job_id
        )
    finally:
        await con.close()

class WorkerSettings:
    functions = [extract_pdf_text]
    redis_settings = RedisSettings(host=os.getenv("REDIS_HOST", "localhost"))