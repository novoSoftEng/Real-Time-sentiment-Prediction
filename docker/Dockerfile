FROM apache/spark-py
USER root
# Copy your Python script into the container
COPY PipelineRL.py /opt/spark/work-dir/pipeline.py
COPY twitter_training.csv /opt/spark/work-dir/twitter_training.csv
COPY consumer.py /opt/spark/work-dir/consumer.py
RUN pip install numpy nltk pandas kafka-python pyspark pymongo
# Define entry point
RUN /opt/spark/bin/spark-submit /opt/spark/work-dir/pipeline.py
CMD ["/usr/bin/python3" , "/opt/spark/work-dir/consumer.py"]

