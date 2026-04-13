from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
import uvicorn
from datetime import datetime
from src import WINDOW_MINUTES, RedisClient, AnalyticsService, windowLogic


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("API Starting up: Initializing Redis connection...")
    yield
    print("API Shutting down: Cleaning up resources...")
    redis_client.stop()
    redis_client.close()

RETRY_DELAY = 5
app = FastAPI(title="KzpExpress API", lifespan=lifespan)
HOST = "127.0.0.1"
PORT = 8000
redis_client = RedisClient(retry_delay=RETRY_DELAY)
analytics_service = AnalyticsService(redis_client)


@app.get(f"/hot-products-last-{WINDOW_MINUTES}-minutes-window")
async def get_hot_products():
    try:
        window_key = windowLogic.get_previous_window_key(datetime.now())
        top_products = analytics_service.get_top_three(window_key)
        return {"window": window_key, "data": top_products}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Analytics service temporarily unavailable: {str(e)}")


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT)