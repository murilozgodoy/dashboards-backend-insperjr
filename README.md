# Backend InsperJr - Dashboards Kaiserhaus

Backend da aplicação de dashboards para análise de dados do restaurante Kaiserhaus, desenvolvido com FastAPI.

## Como executar

### Pré-requisitos
- **Python** (versão 3.8+)
- **Git** (para baixar o projeto)

### Passo a passo

1. **Instale o Python** (se não tiver):
   - Acesse: https://www.python.org/downloads/
   - Baixe a versão **3.8+** (recomendada)
   - Execute o instalador e **marque "Add Python to PATH"**

2. **Baixe o projeto:**
   - Clone o repositório ou baixe o ZIP
   - Extraia em uma pasta de sua escolha

3. **Abra o Terminal/Prompt de Comando:**
   - **Windows**: Pressione `Win + R`, digite `cmd` e pressione Enter
   - **Mac**: Pressione `Cmd + Espaço`, digite "Terminal" e pressione Enter
   - **Linux**: Pressione `Ctrl + Alt + T`

4. **Navegue até a pasta do projeto:**
   ```bash
   cd caminho/para/dashboards-backend-insperjr
   ```

5. **Crie e ative o ambiente virtual:**
   ```bash
   # Criar ambiente virtual
   python -m venv venv
   
   # Ativar ambiente virtual
   # Windows:
   venv\Scripts\activate
   # Mac/Linux:
   source venv/bin/activate
   ```

6. **Instale as dependências:**
   ```bash
   pip install -r requirements.txt
   ```
   *Aguarde terminar (pode demorar alguns minutos na primeira vez)*

7. **Adicione seus dados:**
   - Coloque seus arquivos Excel (`.xlsx`, `.xls`) ou CSV (`.csv`) na pasta `data/`
   - Exemplo: `data/Base_Kaiserhaus_Limpa.csv`

8. **Execute o projeto:**
   ```bash
   python main.py
   ```
   ou
   ```bash
   uvicorn main:app --reload --host 0.0.0.0 --port 8000
   ```

9. **Abra no navegador:**
    - **API**: http://localhost:8000
    - **Documentação**: http://localhost:8000/docs
    - **ReDoc**: http://localhost:8000/redoc

### Se tudo der certo, você verá:
- No terminal: `Uvicorn running on http://0.0.0.0:8000`
- No navegador: A documentação interativa da API

### Problemas comuns:
- **"python não é reconhecido"**: Reinstale o Python e marque "Add to PATH"
- **"pip não é reconhecido"**: Reinstale o Python (pip vem junto)
- **"porta já está em uso"**: Feche outros programas na porta 8000
- **"Arquivo não encontrado"**: Verifique se os arquivos estão na pasta `data/`
- **Ambiente virtual não ativa**: Execute `source venv/bin/activate` (Mac/Linux) ou `venv\Scripts\activate` (Windows)

## Estrutura

```
dashboards-backend-insperjr/
├── data/              # Arquivos de dados (CSV/Excel)
├── main.py            # Aplicação FastAPI principal
├── requirements.txt   # Dependências do projeto
└── venv/              # Ambiente virtual
```

## Scripts

- `python main.py` - Executar aplicação
- `uvicorn main:app --reload` - Executar com reload automático
- `pip install -r requirements.txt` - Instalar dependências

## Endpoints

- `GET /` - Rota raiz
- `GET /health` - Verificar status da API
- `GET /api/dados` - Retornar todos os dados de todos os arquivos
- `GET /api/dados/{nome_arquivo}` - Retornar dados de um arquivo específico
- `GET /api/colunas/{nome_arquivo}` - Retornar informações sobre as colunas de um arquivo

## Tecnologias

- **FastAPI** - Framework web moderno e rápido
- **Pandas** - Manipulação de dados CSV/Excel
- **Uvicorn** - Servidor ASGI
- **Python 3.8+** - Linguagem de programação

## Documentação

- **FastAPI**: https://fastapi.tiangolo.com/
- **Pandas**: https://pandas.pydata.org/
- **Uvicorn**: https://www.uvicorn.org/