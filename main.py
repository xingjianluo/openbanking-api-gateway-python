"""开发入口：uvicorn main:app"""
from obgateway.app import create_app

app = create_app()
