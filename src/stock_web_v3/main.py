"""Main application entry point."""

from .api.routes import create_app

app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "stock_web_v3.main:app",
        host="127.0.0.1",
        port=8000,
        reload=False,
        workers=1
    )