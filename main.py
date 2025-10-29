from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Dict, Any
import pandas as pd
import os
from routes.home import router as home_router

app = FastAPI(title="Dashboard API", version="1.0.0")

#caqui seria a configuracao do cors para permitir requisicoes do front
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_DIR = "data"

@app.get("/")
async def root():
    txt = "API de Dashboards funcionando! Acesse /docs para ver a documentação."
    return {"message": txt}

@app.get("/health")
async def health_check():
    return {"status": "ok", "message": "API está funcionando corretamente"}

# incluir rotas da Home
app.include_router(home_router)

@app.get("/api/dados")
async def get_all_data():
    data_files = []
    
    if not os.path.exists(DATA_DIR):
        return {"error": "Diretório de dados não encontrado", "data": []}
    
    for filename in os.listdir(DATA_DIR):
        if filename.endswith(('.xlsx', '.xls', '.csv')):
            file_path = os.path.join(DATA_DIR, filename)
            
            try:
                if filename.endswith('.csv'):
                    df = pd.read_csv(file_path)
                else:
                    df = pd.read_excel(file_path)
                
                #aqui eh para converter para json
                data = df.to_dict(orient="records")
                
                data_files.append({
                    "arquivo": filename,
                    "quantidade_registros": len(data),
                    "colunas": list(df.columns),
                    "dados": data
                })
            except Exception as e:
                data_files.append({
                    "arquivo": filename,
                    "erro": str(e)
                })
    
    return {"arquivos_processados": len(data_files), "dados": data_files}

#isso seria para buscar dados de um aqruivo,
#caoso for adicionado outro arquivo
@app.get("/api/dados/{nome_arquivo}")
async def get_data_by_file(nome_arquivo: str):
    file_path = os.path.join(DATA_DIR, nome_arquivo)
    
    if not os.path.exists(file_path):
        return {"error": f"Arquivo '{nome_arquivo}' não encontrado"}
    
    try:
        if nome_arquivo.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
        
        data = df.to_dict(orient="records")
        
        return {
            "arquivo": nome_arquivo,
            "quantidade_registros": len(data),
            "colunas": list(df.columns),
            "dados": data
        }
    except Exception as e:
        return {"error": f"Erro ao processar arquivo: {str(e)}"}

#bsucar colunas especificas para ter informacoes sobre elas
@app.get("/api/colunas/{nome_arquivo}")
async def get_columns(nome_arquivo: str):
    file_path = os.path.join(DATA_DIR, nome_arquivo)
    
    if not os.path.exists(file_path):
        return {"error": f"Arquivo '{nome_arquivo}' não encontrado"}
    
    try:
        if nome_arquivo.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
        
        columns_info = []
        for col in df.columns:
            col_data = {
                "nome": col,
                "tipo": str(df[col].dtype),
                "valores_nulos": int(df[col].isna().sum()),
                "valores_unicos": int(df[col].nunique())
            }
            
            if pd.api.types.is_numeric_dtype(df[col]):
                col_data.update({
                    "min": float(df[col].min()),
                    "max": float(df[col].max()),
                    "media": float(df[col].mean()),
                    "mediana": float(df[col].median())
                })
            
            columns_info.append(col_data)
        
        return {
            "arquivo": nome_arquivo,
            "total_registros": len(df),
            "total_colunas": len(df.columns),
            "colunas": columns_info
        }
    except Exception as e:
        return {"error": f"Erro ao processar arquivo: {str(e)}"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)

