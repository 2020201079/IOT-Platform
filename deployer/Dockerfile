FROM python:3
COPY requirements.txt ./
RUN pip install -r requirements.txt
ADD deployer.py .
CMD ["python", "-u","deployer.py"]
