FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY app /app/app

RUN mkdir -p /data

EXPOSE 7071

CMD ["gunicorn", "-w", "2", "--timeout", "120", "-b", "0.0.0.0:7071", "app.app:app"]
