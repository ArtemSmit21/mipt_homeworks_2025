from fastapi import FastAPI
from api.repositories import router as repositories_router

app = FastAPI(
    title="GitHub Repository Search API",
    description="API для поиска репозиториев на GitHub и сохранения результатов в CSV"
)

app.include_router(repositories_router)


@app.get("/")
async def root():
    return {
        "message": "GitHub Repository Search API",
        "docs": "/docs"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
