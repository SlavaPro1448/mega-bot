# Базовый образ: Debian slim + Python 3.10 (совместимо с проектом)
FROM python:3.10-slim

# Переменные окружения для более «тихого» и предсказуемого рантайма
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Установка системных зависимостей
# - megatools (или megadl внутри него) для загрузки с MEGA
# - p7zip-full / unzip для ZIP/7z
# - unar (или unrar-free) для RAR (unar обычно надёжнее)
# - curl для healthcheck, locales для корректной работы UTF-8 путей (папка "аккаунт")
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        locales \
        curl \
        megatools \
        p7zip-full \
        unzip \
        unar \
    && sed -i 's/# *en_US.UTF-8/en_US.UTF-8/' /etc/locale.gen && locale-gen \
    && rm -rf /var/lib/apt/lists/*

ENV LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

# Обновим pip и установим Python-зависимости
COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Копируем исходники
COPY . .

# Создадим нужные каталоги заранее и выставим права
RUN mkdir -p "/app/аккаунт" "/app/downloads" "/app/share" && \
    adduser --disabled-password --gecos "" appuser && \
    chown -R appuser:appuser /app

USER appuser

# Railway/рендеры обычно сами выставляют $PORT,
# твой код слушает 8080 — оставим EXPOSE 8080 для наглядности
EXPOSE 8080

# Healthcheck дергает твой эндпойнт /health
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=5 \
  CMD curl -fsS http://127.0.0.1:8080/health || exit 1

# Запуск бота + встроенного aiohttp-сервера (у тебя всё в одном процессе)
CMD ["python", "-u", "razarhivator.py"]