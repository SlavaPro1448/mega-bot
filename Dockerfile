FROM python:3.10

WORKDIR /app

RUN apt-get update && \ 
    apt-get install -y megatools p7zip-full rar unrar unzip patool &&     pip install --no-cache-dir aiogram pyunpack patool

COPY . .

CMD ["python", "razarhivator.py"]