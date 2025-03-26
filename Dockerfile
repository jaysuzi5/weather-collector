# Use a slim Python 3.12 base image
FROM python:3.12.9-slim-bookworm

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
COPY src/ /app/src/

# Set environment variables
ENV OPENWEATHER_API_KEY=tbd
ENV LATITUDE=40.7128
ENV LONGITUDE=-74.0060
ENV WEATHER_SLEEP_SECONDS=300

# Command to run the application
CMD ["python", "/app/src/weather-collector.py"]