FROM bitnami/spark

USER root
RUN pip install unidecode
RUN curl https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/0.13.0/spark-xml_2.12-0.13.0.jar --output /opt/bitnami/spark/jars/spark-xml_2.10-0.2.0.jar
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9-src.zip:$PYTHONPATH