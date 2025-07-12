# Use an official Python runtime as a parent image
FROM python:3.12-slim-bookworm

# Set the working directory in the container
WORKDIR /app

# Remove system dependencies not needed for connecting to an external DB
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     postgresql-client \ # No longer needed for pg_isready
#     locales \ # No longer needed for locale-gen
#     && rm -rf /var/lib/apt/lists/*

# If you still want to suppress locale warnings, you can keep these,
# but they are not strictly necessary for functionality.
# RUN locale-gen en_US.UTF-8
# ENV LANG en_US.UTF-8
# ENV LANGUAGE en_US:en
# ENV LC_ALL en_US.UTF-8


# Copy the requirements file into the working directory
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the 'monitor' package (which contains .env, constants.py etc.)
# from your host's ./monitor to the container's /app/monitor
COPY ./monitor /app/monitor

# Copy main.py from the root of your build context (your project root)
# to the container's /app directory
COPY ./main.py /app/main.py

# Remove copying and executing start.sh as it's no longer needed for DB waiting
# COPY start.sh /app/start.sh
# RUN chmod +x /app/start.sh

# Expose the port that FastAPI will run on
EXPOSE 666

# Command to run the application using Uvicorn directly
# UPDATED: Uvicorn runs directly, connecting to the host's DB
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "666"]
