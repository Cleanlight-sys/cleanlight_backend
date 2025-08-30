# schema/base.py
import os

base = {
    "openapi": "3.1.0",
    "info": {
        "title": "Cleanlight Agent API",
        "version": "1.3",
        "description": "Single-source schema. All operations through `/query`. `/hint` available for examples."
    },
    "servers": [
        {
            "url": os.getenv("RENDER_EXTERNAL_URL", "https://cleanlight-backend.onrender.com")
        }
    ]
}
