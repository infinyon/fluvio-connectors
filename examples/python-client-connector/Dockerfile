FROM python

# Copy our python script into the connector image
COPY get-cat-facts.py /usr/local/sbin/get-cat-facts.py
RUN chmod +x /usr/local/sbin/get-cat-facts.py

# This is required to connect to a cluster
# Connectors run as the `fluvio` user
ENV USER=fluvio
RUN useradd --create-home "$USER"
USER $USER

# Install dependencies
RUN pip install fluvio requests

# Start script on start
ENTRYPOINT get-cat-facts.py