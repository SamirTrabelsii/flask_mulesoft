# Use Debian-based Python 3.9 slim-buster image
FROM python:3.9-slim-buster

# Set working directory in the container
WORKDIR /app

# Copy the requirements.txt file into the container at /app
COPY main.py config.xml requirements.txt ./

# Install Python dependencies including Gunicorn
RUN pip install --no-cache-dir -r requirements.txt
# Install Gunicorn
RUN pip install gunicorn

# Set environment variables
ENV FLASK_APP=main.py
ENV FLASK_ENV=production

# Expose the port on which the Flask app will run
EXPOSE 5000

# Command to run the Flask app using Gunicorn
CMD ["gunicorn", "-b", "0.0.0.0:5000", "main:app"]

