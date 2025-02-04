# Use a lighter base image (e.g., ubuntu:20.04)
FROM ubuntu:20.04

# Set non-interactive mode for package installations
ENV DEBIAN_FRONTEND=noninteractive
USER root
# Install system dependencies
RUN apt-get update \
    && apt-get install -y curl \
                           --no-install-recommends \
                           build-essential \
                           r-base \
                           r-cran-forecast \
                           python3.8 \
                           python3-pip \
                           python3.8-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install R packages
RUN Rscript -e "install.packages(c('forecast', 'jsonlite', 'tidyr', 'dplyr'))"

# Install Python dependencies
COPY requirements.txt .
RUN pip install --upgrade setuptools==59.0.1 pip==21.1.3 \
    && pip install --no-cache-dir -r requirements.txt

# Create the working directory
WORKDIR /forecast-app

# Copy configuration, app, and source code
COPY config_files/* /forecast-app/config_files/
COPY *.py /forecast-app/
COPY forecastApi /forecast-app/forecastApi/
COPY dataPipeline /forecast-app/dataPipeline/
COPY dataProcessing /forecast-app/dataProcessing/
COPY modelling /forecast-app/modelling/
COPY testcases /forecast-app/testcases/
COPY support_files /forecast-app/support_files/
COPY home.txt /forecast-app/home.txt

EXPOSE 5000

CMD ["python3", "-m", "flask", "run", "--host=0.0.0.0", "--port=5002"]
