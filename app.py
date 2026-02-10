from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.requests import Request
import shutil
import os
import utils

app = FastAPI(title="KML to Excel Master Pop Up")
templates = Jinja2Templates(directory="templates")

os.makedirs("temp", exist_ok=True)

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/process")
async def process_file(file: UploadFile = File(...)):
    temp_path = f"temp/{file.filename}"
    # Nama file output excel
    output_filename = f"MASTER_POP_UP_RESULT_{file.filename}.xlsx"
    output_path = f"temp/{output_filename}"
    
    try:
        # Simpan file upload
        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        # Proses Data
        df_result = utils.process_design(temp_path)
        
        # Export ke Excel (.xlsx)
        df_result.to_excel(output_path, index=False)
        
        return FileResponse(
            path=output_path, 
            filename=output_filename,
            media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        return {"error": str(e), "message": "Gagal memproses. Pastikan file KML valid."}