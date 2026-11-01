FROM spark:python3

USER root

ENV SPARK_HOME=/opt/spark
ENV HOME=/tmp
ENV PYTHONPATH=/opt/spark/python:$PYTHONPATH

RUN pip install --no-cache-dir \
    jupyterlab \
    py4j \
    pandas \
    matplotlib

WORKDIR /opt/project

RUN mkdir -p /tmp/jupyter/runtime /tmp/jupyter/data \
    && chmod -R 777 /tmp/jupyter

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]