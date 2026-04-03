FROM jupyter/pyspark-notebook:latest

COPY requirements.txt /tmp/requirements.txt
COPY work /home/jovyan/work

RUN pip install --no-cache-dir -r /tmp/requirements.txt \
	&& pip install --no-cache-dir -e /home/jovyan/work