from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from pathlib import Path

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

build_dir = Path(__file__).parent.parent / "frontend" / "dist"


@app.head("/")
async def liveness():
    """Platform proxy sends HEAD / to verify the server is up."""
    return JSONResponse({"status": "ok"})


@app.get("/healthz")
async def healthz():
    """Health check endpoint."""
    return {"status": "ok"}


# --- Add your API routes below ---


# Serve frontend build with SPA catch-all (must be last)
if build_dir.exists():

    @app.get("/{full_path:path}")
    async def spa(full_path: str = ""):
        """Serve index.html for all non-API routes (SPA client-side routing)."""
        file = (build_dir / full_path).resolve()
        if file.is_file() and file.is_relative_to(build_dir.resolve()):
            return FileResponse(str(file))
        return FileResponse(str(build_dir / "index.html"))

else:

    @app.get("/")
    async def no_build():
        return JSONResponse(
            {"error": "Frontend build not found. Run 'npm run build' in frontend/."},
            status_code=503,
        )
