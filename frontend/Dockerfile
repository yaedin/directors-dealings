# frontend Dockerfile

FROM python:3-slim

WORKDIR /usr/src/

ENV GIT_PYTHON_REFRESH=quiet

COPY requirements.txt .
RUN pip install -r requirements.txt

EXPOSE 8501

CMD ["streamlit", "run", "docker_practice/frontend/main.py"]
