FROM postgres:17

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --break-system-packages boto3

COPY storage.py archiver.py backup_db.py restore_db.py /scripts/

ENTRYPOINT ["python3", "/scripts/backup_db.py"]
