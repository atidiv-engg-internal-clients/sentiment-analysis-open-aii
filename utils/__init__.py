import os
from dotenv import load_dotenv

from .bq import GoogleBigQuery

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
