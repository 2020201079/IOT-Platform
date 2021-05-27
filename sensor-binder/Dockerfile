FROM python:3
COPY requirements.txt ./
RUN pip install -r requirements.txt
ADD sensor_binder.py .
CMD ["python", "-u","sensor_binder.py"]
