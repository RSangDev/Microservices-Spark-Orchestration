FROM python:3.11-alpine
RUN pip install --no-cache-dir flask==3.0.2
WORKDIR /app
COPY mock_server.py .
CMD ["python", "mock_server.py"]