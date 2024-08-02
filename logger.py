import logging

# Configure the logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level to INFO
    format="%(asctime)s - %(levelname)s - %(message)s",  # Format the log messages
    handlers=[
        logging.FileHandler("logfile.log"),  # Log messages to a file named 'app.log'
        logging.StreamHandler(),  # Also log messages to the console
    ],
)
