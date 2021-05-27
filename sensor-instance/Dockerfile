FROM python:3
WORKDIR /usr/src/app
COPY . .
RUN pip3 install Flask
RUN pip3 install pymongo
RUN pip3 install kafka
RUN pip3 install dnspython
RUN pip3 install kafka_python
RUN pip3 install faker
RUN pip3 install Faker
CMD ["sensor_instance_registration.py"]
ENTRYPOINT ["python3"]
