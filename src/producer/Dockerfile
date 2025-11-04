FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY generate_payments.py ./generate_payments.py
CMD ["python", "generate_payments.py"]
