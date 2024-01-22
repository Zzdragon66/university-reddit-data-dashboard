FROM nvidia/cuda:12.3.1-base-ubuntu20.04

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3-pip \
        python3-dev

RUN python3 -m pip install --upgrade pip

WORKDIR /image_caption

COPY ./python_requirements.txt ./python_requirements.txt
COPY ./image_caption.py ./image_caption.py
COPY ./gcp_key.json ./gcp_key.json
RUN pip3 install -r ./python_requirements.txt

ENTRYPOINT [ "python3", "-u", "./image_caption.py"]
CMD []