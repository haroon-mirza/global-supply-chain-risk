FROM python:3.15-slim
WORKDIR /app

# Copy requirements and installs
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy Python script
COPY commtrade_hhi_pipeline.py .

# Run script
CMD ["python", "commtrade_hhi_pipeline.py"]