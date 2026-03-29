FROM python:3.12-slim

WORKDIR /app

# Install deps first (cached layer)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create local hub_data dir (used as fallback when GitHub write fails)
RUN mkdir -p hub_data

EXPOSE 8080

CMD ["gunicorn", "--config", "gunicorn.conf.py", "app:app"]
