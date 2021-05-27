FROM python:2
WORKDIR /usr/src/app
COPY . .
RUN pip install Flask
RUN pip install pymongo
RUN pip install kafka
RUN pip install dnspython
CMD ["platformManager.py"]
ENTRYPOINT ["python"]

