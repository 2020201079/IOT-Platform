FROM python:2
WORKDIR /usr/src/app
COPY . .
RUN pip install Flask
RUN pip install pymongo
RUN pip install kafka
RUN pip install dnspython
CMD ["sensor_type_registration.py"]
ENTRYPOINT ["python"]
