from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.head("/")
async def liveness():
    """Platform proxy sends HEAD / to verify the server is up."""
    return JSONResponse({"status": "ok"})


@app.get("/healthz")
async def healthz():
    """Health check endpoint."""
    return {"status": "ok"}


# --- Add your API routes below ---


# Serve static files (must be last — catches all unmatched routes)
app.mount("/", StaticFiles(directory="static", html=True), name="static")
