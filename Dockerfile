FROM python:3.12-slim-bookworm


WORKDIR /app

COPY requirements.txt .


RUN pip install --no-cache-dir -r requirements.txt

COPY ./monitor /app/monitor


COPY ./main.py /app/main.py


EXPOSE 666


CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "666"]
