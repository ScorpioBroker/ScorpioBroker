# syntax=docker/dockerfile:1
FROM python:3
ENV PYTHONUNBUFFERED=1
WORKDIR /dashboard

COPY assets /dashboard/assets
COPY * /dashboard/
RUN pip install -r requirements.txt
CMD python dashboard.py
