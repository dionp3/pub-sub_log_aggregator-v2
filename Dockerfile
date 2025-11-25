FROM python:3.11-slim
WORKDIR /app

# Mengatur PYTHONPATH agar modul src dapat diimpor
ENV PYTHONPATH=/app

# Copy dan instal dependensi
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Menambahkan user non-root (T10: Keamanan)
RUN adduser --disabled-password --gecos '' appuser 

# Copy kode sumber dan berikan kepemilikan
COPY src/ ./src/ 
RUN chown -R appuser:appuser /app
COPY tests/ ./tests/

USER appuser

EXPOSE 8080

# Command default untuk Aggregator
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080", "--app-dir", "/app"]