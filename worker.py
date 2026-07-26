import sqlite3, asyncio
from pypdf import PdfReader
from arq import Worker
from arq.connections import RedisSettings
import fitz

job_semaphore = asyncio.Semaphore(3)

async def extract_pdf_text(ctx, job_id: str, file_path: str):
    async with job_semaphore:
        try:
            con = sqlite3.connect("jobs.db")
            cur = con.cursor()
            update_statement = 'UPDATE jobs SET status = ? WHERE id = ?'
            cur.execute(update_statement, ("processing", job_id))
            con.commit()
            con.close()
            text = ""
            with fitz.open(file_path) as doc:
                for page in doc:
                    text += page.get_text()
            if not text.strip():
                raise ValueError("No selectable text found. Scanned documents are not supported.")
            con = sqlite3.connect("jobs.db")
            cur = con.cursor()
            update_statement = 'UPDATE jobs SET status = ?, extracted_text = ? WHERE id = ?'
            cur.execute(update_statement, ("completed", text, job_id))
            con.commit()
            con.close()
        except Exception as e:
            con = sqlite3.connect("jobs.db")
            cur = con.cursor()
            update_statement2 = 'UPDATE jobs SET status = ?, error_detail = ? WHERE id = ?'
            cur.execute(update_statement2, ("failed", str(e), job_id))
            con.commit()
            con.close()
            pass

class WorkerSettings:
    functions = [extract_pdf_text]
    redis_settings = RedisSettings()