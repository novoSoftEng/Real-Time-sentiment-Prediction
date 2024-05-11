FROM apache/spark-py
USER root
# Copy your Python script into the container
COPY PipelineRL.py /opt/spark/work-dir/pipeline.py
COPY twitter_training.csv /opt/spark/work-dir/twitter_training.csv
RUN pip install numpy nltk pandas kafka-python pyspark pymongo flask
COPY api.py /opt/spark/work-dir/api.py 
EXPOSE 8080
EXPOSE 5000
# Define entry point

RUN /opt/spark/bin/spark-submit /opt/spark/work-dir/pipeline.py
#CMD ["/opt/spark/bin/python" , "/opt/spark/work-dir/consumer.py"]

CMD ["python3" ,"/opt/spark/work-dir/api.py"]