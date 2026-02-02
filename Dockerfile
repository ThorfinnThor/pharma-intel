FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

ENV PHARMA_INTEL_DB_URL=sqlite:///data/intel.db

EXPOSE 8000
CMD ["python","-m","intel.cli","serve","--host","0.0.0.0","--port","8000"]
