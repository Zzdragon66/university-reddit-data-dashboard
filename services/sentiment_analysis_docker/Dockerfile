FROM nvidia/cuda:12.3.1-base-ubuntu20.04

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3-pip \
        python3-dev

RUN python3 -m pip install --upgrade pip

WORKDIR /sentiment_analysis

COPY ./python_requirements.txt ./python_requirements.txt
RUN pip3 install -r ./python_requirements.txt
COPY ./sentiment_analysis.py ./sentiment_analysis.py
COPY ./gcp_key.json ./gcp_key.json

ENTRYPOINT [ "python3", "-u", "./sentiment_analysis.py"]
CMD []