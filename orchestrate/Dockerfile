FROM python:3.7

ENV ORCHESTRATOR_MODE=live

WORKDIR /orchestrate

COPY requirements.txt ./
RUN pip install -r requirements.txt

COPY . ./

RUN pip install -e .
