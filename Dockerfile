FROM python:3.12-slim
WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt
ENV PORT=10000
CMD ["sh","-c","gunicorn --bind 0.0.0.0:${PORT:-10000} --workers 1 --threads 6 --timeout 120 --graceful-timeout 20 app:app"]
