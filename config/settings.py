import os

SECRET_KEY = os.getenv('SECRET_KEY', None)

SQLALCHEMY_DATABASE_URI = 'postgresql://{0}:{1}@localhost:5432/{2}'.format(
    os.getenv('POSTGRES_USER'),
    os.getenv('POSTGRES_PASSWORD'),
    os.getenv('POSTGRES_DB')
)
SQLALCHEMY_TRACK_MODIFICATIONS = False
