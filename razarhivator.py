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
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery

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

    user_id = str(message.from_user.id)
    user_output_dir = Path(OUTPUT_DIR) / user_id
    user_output_dir.mkdir(parents=True, exist_ok=True)

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
                extract_dir = os.path.join(user_output_dir, file_path.stem)
                os.makedirs(extract_dir, exist_ok=True)
                recursively_unpack(file_path, extract_dir)
            else:
                new_path = user_output_dir / file_path.name
                os.rename(file_path, new_path)

            if 'extract_dir' in locals() and os.path.exists(extract_dir):
                for root, dirs, files in os.walk(extract_dir, topdown=False):
                    for name in files:
                        fpath = Path(root) / name
                        if fpath.suffix.lower() in ['.json', '.session']:
                            os.rename(fpath, user_output_dir / fpath.name)
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

    final_files = [f for f in user_output_dir.iterdir() if f.is_file() and f.suffix.lower() in ['.json', '.session']]

    if not final_files:
        await message.reply("Готово! Но не найдено файлов .json или .session.")
    else:
        share_id = str(uuid.uuid4())
        share_folder = Path("/app/share") / user_id / share_id
        share_folder.mkdir(parents=True, exist_ok=True)
        for f in final_files:
            shutil.copy(f, share_folder / f.name)

        # Удаление через 30 минут
        async def delayed_cleanup(folder, delay=1800):
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

        download_link = f"https://{os.getenv('RAILWAY_STATIC_URL')}/download/{user_id}/{share_id}"
        archive_message = await message.reply(f"Готово! Вот ссылка для скачивания ZIP-архива:\n{download_link}")
        buttons = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📥 Скачать снова", url=download_link)],
            [InlineKeyboardButton(text="🗑 Удалить архив", callback_data="delete_last")]
        ])
        await message.reply("Что хотите сделать дальше?", reply_markup=buttons)

@dp.message()
async def fallback(message: types.Message):
    await message.reply("Используй команду /start и отправь ссылки на MEGA.")

@dp.callback_query(lambda c: c.data == "upload_more")
async def handle_upload_more(callback_query: CallbackQuery, state: FSMContext):
    await callback_query.message.answer("Хорошо, отправьте новые ссылки на MEGA:")
    await state.set_state(DownloadState.waiting_for_link)
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "delete_last")
async def handle_delete_last(callback_query: CallbackQuery):
    import shutil
    from glob import glob

    # Удаляем архивы .zip, связанные с пользователем
    user_id = str(callback_query.from_user.id)
    user_zip_dir = Path("/app/share") / user_id
    for zip_file in user_zip_dir.glob("**/*.zip"):
        try:
            zip_file.unlink()
        except Exception as e:
            logging.warning(f"Не удалось удалить архив: {zip_file} — {e}")

    try:
        await callback_query.message.delete()
    except Exception as e:
        logging.warning(f"Не удалось удалить сообщение: {str(e)}")

    # Удаляем сообщение со ссылкой, если возможно
    async for msg in callback_query.message.chat.get_history(limit=5):
        if msg.text and "ссылка для скачивания" in msg.text:
            try:
                await msg.delete()
                break
            except Exception as e:
                logging.warning(f"Не удалось удалить сообщение со ссылкой: {str(e)}")

    user_folder = Path("/app/аккаунт") / user_id
    share_folder = Path("/app/share") / user_id
    try:
        if user_folder.exists():
            shutil.rmtree(user_folder)
        if share_folder.exists():
            shutil.rmtree(share_folder)
        zip_files = list(Path("/app/share") / user_id).glob("**/*.zip")
        for zip_file in zip_files:
            try:
                zip_file.unlink()
            except Exception as e:
                logging.warning(f"Не удалось удалить zip-файл: {zip_file} — {e}")
        await callback_query.message.answer("Последние файлы и архивы удалены.")
    except Exception as e:
        await callback_query.message.answer(f"Ошибка при удалении: {str(e)}")
    await callback_query.answer()

async def main():

    async def handle_download(request):
        token1 = request.match_info.get("token1")
        token2 = request.match_info.get("token2")
        folder = Path("/app/share") / token1 / token2
        if folder.exists() and folder.is_dir():
            zip_path = folder.with_suffix(".zip")
            if not zip_path.exists():
                shutil.make_archive(str(zip_path.with_suffix("")), 'zip', folder)
            return web.FileResponse(path=zip_path)
        return web.Response(status=404, text="Not found")

    app = web.Application()
    app.router.add_get("/download/{token1}/{token2}", handle_download)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, port=8080)
    await site.start()

    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
