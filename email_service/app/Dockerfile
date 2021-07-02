 
FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8
COPY . /app/app
RUN pip install --no-cache-dir fastapi uvicorn psycopg2-binary 
CMD ["uvicorn", "app.app:app", "--host", "0.0.0.0", "--port", "80"]
