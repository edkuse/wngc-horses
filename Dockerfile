FROM python:3.7.3-alpine

WORKDIR /app

COPY requirements.txt requirements.txt

RUN \
  apk add --no-cache postgresql-libs && \
  apk add --no-cache --virtual .build-deps gcc musl-dev postgresql-dev && \
  pip install -r requirements.txt && \
  apk --purge del .build-deps

ARG FLASK_ENV="production"
ENV FLASK_ENV="${FLASK_ENV}" \
    PYTHONUNBUFFERED="true"

COPY . .

# Run the image as a non-root user
RUN adduser -D wngcuser
USER wngcuser

# Run the app.  CMD is required to run on Heroku
# $PORT is set by Heroku
ENV PORT=8000		
CMD gunicorn -b 0.0.0.0:$PORT "wngchorses.app:create_app()"
