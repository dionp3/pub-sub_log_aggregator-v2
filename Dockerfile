FROM python:3.11-slim
WORKDIR /app

ENV PYTHONPATH=/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN adduser --disabled-password --gecos '' appuser 

COPY src/ ./src/ 
RUN chown -R appuser:appuser /app
COPY tests/ ./tests/

USER appuser

EXPOSE 8080

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080", "--app-dir", "/app"]