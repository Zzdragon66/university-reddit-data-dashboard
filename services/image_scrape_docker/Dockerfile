FROM python:3.11.7

WORKDIR /usr/src/scrape_image
ENV PYTHONUNBUFFERED=1

#copy the gcp_key.json file into the directory
COPY ./gcp_key.json ./gcp_key.json

# Install the python requirements
COPY ./python_requirements.txt ./python_requirements.txt
RUN pip3 install --no-cache-dir -r python_requirements.txt

# copy the reddit scraping file 
COPY ./image_scraping.py ./image_scraping.py

ENTRYPOINT ["python3", "-u", "./image_scraping.py"]
CMD []