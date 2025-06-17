import os
import logging
import asyncio
import subprocess
import uuid
import shutil
import time
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command, StateFilter
from pathlib import Path
from pyunpack import Archive

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Получаем токен из переменной окружения
API_TOKEN = os.getenv("BOT_TOKEN")
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Папки для загрузки и выгрузки
OUTPUT_DIR = str(Path("/app/аккаунт"))
DOWNLOAD_DIR = '/app/downloads'

# Создаем директории
for directory in [OUTPUT_DIR, DOWNLOAD_DIR]:
    os.makedirs(directory, exist_ok=True)

# Состояния
class DownloadState(StatesGroup):
    waiting_for_link = State()

# Рекурсивная распаковка
def recursively_unpack(archive_path, extract_dir):
    try:
        Archive(str(archive_path)).extractall(extract_dir)
        os.remove(archive_path)
        for file in os.listdir(extract_dir):
            file_path = Path(extract_dir) / file
            if file_path.is_file() and file_path.suffix.lower() in ['.zip', '.rar', '.7z']:
                recursively_unpack(file_path, extract_dir)
    except Exception as e:
        logging.error(f"Ошибка при разархивации: {str(e)}")

# /start
@dp.message(Command(commands=['start']))
async def send_welcome(message: types.Message, state: FSMContext):
    await message.reply("Привет! Отправь мне ссылки на MEGA — я их скачаю и разархивирую.")
    await state.set_state(DownloadState.waiting_for_link)

# обработка ссылок
@dp.message(StateFilter(DownloadState.waiting_for_link))
async def process_link(message: types.Message, state: FSMContext):
    import re
    links = re.findall(r'https?://mega\.nz/\S+', message.text)
    if not links:
        await message.reply("Не найдено ни одной ссылки на MEGA.")
        return

    for file in os.listdir(DOWNLOAD_DIR):
        file_path = Path(DOWNLOAD_DIR) / file
        if file_path.is_file():
            os.remove(file_path)

    for link in links:
        try:
            result = subprocess.run(['megatools', 'dl', '--path', str(DOWNLOAD_DIR), link], capture_output=True, text=True)
            if result.returncode != 0:
                await message.reply(f"Ошибка при скачивании: {result.stderr}")
                continue

            downloaded_files = sorted(
                [f for f in os.listdir(DOWNLOAD_DIR) if (Path(DOWNLOAD_DIR) / f).is_file()],
                key=lambda f: os.path.getmtime(Path(DOWNLOAD_DIR) / f),
                reverse=True
            )
            if not downloaded_files:
                await message.reply("Файл не был скачан.")
                continue

            file_path = Path(DOWNLOAD_DIR) / downloaded_files[0]
            if file_path.suffix.lower() in ['.zip', '.rar', '.7z']:
                extract_dir = os.path.join(OUTPUT_DIR, file_path.stem)
                os.makedirs(extract_dir, exist_ok=True)
                recursively_unpack(file_path, extract_dir)
            else:
                new_path = Path(OUTPUT_DIR) / file_path.name
                os.rename(file_path, new_path)

            if 'extract_dir' in locals() and os.path.exists(extract_dir):
                for root, dirs, files in os.walk(extract_dir, topdown=False):
                    for name in files:
                        fpath = Path(root) / name
                        if fpath.suffix.lower() in ['.json', '.session']:
                            os.rename(fpath, Path(OUTPUT_DIR) / name)
                        else:
                            os.remove(fpath)
                    for d in dirs:
                        try:
                            os.rmdir(Path(root) / d)
                        except OSError:
                            pass
                try:
                    os.rmdir(extract_dir)
                except OSError:
                    pass

        except Exception as e:
            await message.reply(f"Ошибка: {str(e)}")

    final_files = [f for f in Path(OUTPUT_DIR).iterdir() if f.is_file() and f.suffix.lower() in ['.json', '.session']]

    if not final_files:
        await message.reply("Готово! Но не найдено файлов .json или .session.")
    else:
        share_id = str(uuid.uuid4())
        share_folder = Path("/app/share") / share_id
        share_folder.mkdir(parents=True, exist_ok=True)
        for f in final_files:
            shutil.copy(f, share_folder / f.name)

        # Планируем удаление папки через 30 минут
        async def delayed_cleanup(folder, delay=1800):  # 1800 секунд = 30 минут
            await asyncio.sleep(delay)
            try:
                shutil.rmtree(folder)
                zip_path = folder.with_suffix(".zip")
                if zip_path.exists():
                    zip_path.unlink()
                logging.info(f"Удалена временная папка и zip: {folder}")
            except Exception as e:
                logging.error(f"Ошибка при удалении: {str(e)}")

        asyncio.create_task(delayed_cleanup(share_folder))

        download_link = f"https://{os.getenv('RAILWAY_STATIC_URL')}/download/{share_id}"
        await message.reply(f"Готово! Вот ссылка для скачивания ZIP-архива:\n{download_link}")

@dp.message()
async def fallback(message: types.Message):
    await message.reply("Используй команду /start и отправь ссылки на MEGA.")

async def main():

    async def handle_download(request):
        token = request.match_info.get("token")
        folder = Path("/app/share") / token
        if folder.exists() and folder.is_dir():
            zip_path = folder.with_suffix(".zip")
            if not zip_path.exists():
                shutil.make_archive(str(zip_path.with_suffix("")), 'zip', folder)
            return web.FileResponse(path=zip_path)
        return web.Response(status=404, text="Not found")

    app = web.Application()
    app.router.add_get("/download/{token}", handle_download)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, port=8080)
    await site.start()

    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
