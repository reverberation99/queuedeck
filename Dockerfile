FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY app /app/app

RUN mkdir -p /data

EXPOSE 7071

CMD ["gunicorn", "-w", "1", "-k", "gthread", "--threads", "8", "--timeout", "300", "-b", "0.0.0.0:7071", "app.app:app"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:7071/healthz', timeout=3)" || exit 1
