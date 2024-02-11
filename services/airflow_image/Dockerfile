FROM apache/airflow:latest-python3.10

USER airflow
COPY ./*.json /opt/airflow/
COPY ./python_requirements.txt opt/airflow/python_requirements.txt
COPY ./reddit_ssh /opt/airflow/reddit_ssh
COPY ./reddit_ssh.pub /opt/airflow/reddit_ssh.pub
COPY ./subreddits.txt /opt/airflow/subreddits.txt
RUN pip3 install -r opt/airflow/python_requirements.txt

USER root
RUN chmod -R 775 /opt/airflow/

USER airflow