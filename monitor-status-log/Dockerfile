FROM python:3
COPY requirements.txt ./
RUN pip install -r requirements.txt
ADD fetch_status.py .
ADD config.json .
CMD ["python", "-u","fetch_status.py"]
