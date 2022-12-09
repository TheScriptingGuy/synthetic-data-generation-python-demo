# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.10-slim-buster

EXPOSE 8000

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1 

RUN apt-get update && apt-get install git-all -y

COPY / /app
WORKDIR /app

#RUN python3 -m pip install --upgrade build
RUN python -m pip install -r requirements.txt

# Switching to a non-root user, please refer to https://aka.ms/vscode-docker-python-user-rights
RUN useradd appuser && chown -R appuser /app
USER appuser
 
# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
#CMD ["python", "manage.py", "runserver","0.0.0.0:8000"]
