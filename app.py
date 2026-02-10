from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
import shutil
import os
import utils

app = FastAPI(title="Auto Design KMZ to CSV")

# Setup Folder Templates
templates = Jinja2Templates(directory="templates")

# Pastikan folder temp ada
os.makedirs("temp", exist_ok=True)

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/process")
async def process_file(file: UploadFile = File(...)):
    temp_file_path = f"temp/{file.filename}"
    output_csv_path = f"temp/processed_{file.filename}.csv"
    
    try:
        # 1. Simpan file upload ke server sementara
        with open(temp_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        # 2. Proses menggunakan logic di utils.py
        df_result = utils.process_design(temp_file_path)
        
        # 3. Export ke CSV
        df_result.to_csv(output_csv_path, index=False)
        
        # 4. Return file ke user
        return FileResponse(
            path=output_csv_path, 
            filename=f"RESULT_{file.filename}.csv",
            media_type='text/csv'
        )

    except Exception as e:
        return {"error": str(e), "message": "Pastikan file KML memiliki folder: HOMEPASS, FAT, dan POLE"}
    
    finally:
        # Opsional: Bersihkan file temp (bisa diaktifkan jika perlu)
        pass