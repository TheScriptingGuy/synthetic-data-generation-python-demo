# For more information, please refer to https://aka.ms/vscode-docker-python
FROM python:3.8-slim-buster

EXPOSE 8000

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1 

#apt update, install git and install graphviz for visualization
RUN apt-get update && apt-get install git-all -y && apt-get install graphviz



WORKDIR /app
# Install pip requirements
COPY requirements.txt .
RUN python -m pip install -r requirements.txt

COPY / /app

RUN git clone https://github.com/DataResponsibly/dataResponsiblyUI.git
WORKDIR dataResponsiblyUI
RUN python manage.py migrate

EXPOSE 8000

# Switching to a non-root user, please refer to https://aka.ms/vscode-docker-python-user-rights
RUN useradd appuser && chown -R appuser /app
USER appuser
 
# During debugging, this entry point will be overridden. For more information, please refer to https://aka.ms/vscode-docker-python-debug
CMD ["python", "manage.py", "runserver","0.0.0.0:8000"]
