FROM python:3
COPY requirements.txt ./
RUN pip install -r requirements.txt
ADD app_monitoring.py .
ADD server_details.json .
CMD ["python", "-u","app_monitoring.py"]
