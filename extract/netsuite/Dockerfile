FROM python:3.7

COPY requirements.txt ./
RUN pip install -U pip
RUN pip install -r requirements.txt

COPY . ./netsuite
ENV PYTHONPATH "${PYTHONPATH}:netsuite/"

RUN pip install -e netsuite/shared_modules/

