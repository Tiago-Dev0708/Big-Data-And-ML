FROM python:3.9-slim

RUN apt-get update && \
    apt-get install -y default-jdk libgomp1 && \
    apt-get clean;

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./app /app/app

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
