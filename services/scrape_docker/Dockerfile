FROM python:3.11.7

WORKDIR /usr/src/reddit_scraping
ENV PYTHONUNBUFFERED=1

#copy the gcp_key.json file into the directory
COPY ./gcp_key.json ./gcp_key.json

# Install the python requirements
COPY ./requirements.txt /usr/src/reddit_scraping/requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# copy the reddit scraping file 
COPY ./reddit_scraping.py /usr/src/reddit_scraping/reddit_scraping.py

ENTRYPOINT ["python3", "-u", "/usr/src/reddit_scraping/reddit_scraping.py"]
CMD []