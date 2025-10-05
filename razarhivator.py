import os
import logging
import asyncio
import subprocess
import uuid
import shutil
import time
import json
import hmac
import hashlib
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.filters import Command, StateFilter
from pathlib import Path
from pyunpack import Archive
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from datetime import datetime, timedelta
import stripe

# Настройка логирования
logging.basicConfig(level=logging.INFO)

# Получаем токены и настройки из переменных окружения
API_TOKEN = os.getenv("BOT_TOKEN")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL")
STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")
STRIPE_PRICE_ID = os.getenv("STRIPE_PRICE_ID")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
MAX_UNPACK_BYTES = int(os.getenv("MAX_UNPACK_BYTES", "0"))  # 0 = без ограничений

# Количество бесплатных дней триала (0 = без триала)
STRIPE_TRIAL_DAYS = int(os.getenv("STRIPE_TRIAL_DAYS", "0"))

# Секретный токен для Telegram webhook
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")

# Нормализатор base URL
def _base_url() -> str:
    url = (PUBLIC_BASE_URL or "").strip()
    if url and not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url.rstrip('/')

# Настройка Stripe
stripe.api_key = STRIPE_SECRET_KEY

bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Папки для загрузки и выгрузки
OUTPUT_DIR = str(Path("/app/аккаунт"))
DOWNLOAD_DIR = '/app/downloads'

# Создаем директории
for directory in [OUTPUT_DIR, DOWNLOAD_DIR]:
    os.makedirs(directory, exist_ok=True)

# Файл для хранения лицензий
LICENSES_FILE = os.getenv("LICENSES_FILE", "/app/licenses.json")
logging.info(f"Licenses file path: {LICENSES_FILE}")

# Функции для работы с лицензиями
def load_licenses():
    """Загружает данные лицензий из JSON файла"""
    try:
        if os.path.exists(LICENSES_FILE):
            with open(LICENSES_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {"users": {}, "pending_by_email": {}, "subs": {}}
    except Exception as e:
        logging.error(f"Ошибка загрузки лицензий: {e}")
        return {"users": {}, "pending_by_email": {}, "subs": {}}

def save_licenses(data):
    """Сохраняет данные лицензий в JSON файл"""
    try:
        with open(LICENSES_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logging.error(f"Ошибка сохранения лицензий: {e}")
        return False

def is_license_active(user_id):
    """Проверяет, активна ли лицензия пользователя"""
    try:
        # Администратор всегда имеет доступ
        try:
            if int(user_id) == int(ADMIN_ID):
                return True
        except Exception:
            pass
        licenses = load_licenses()
        user_id_str = str(user_id)
        
        if user_id_str not in licenses["users"]:
            return False
            
        user_data = licenses["users"][user_id_str]
        expires_ts = user_data.get("expires_ts", 0)
        grace_until = user_data.get("grace_until", 0)
        current_time = int(time.time())
        
        # Проверяем основную подписку
        if expires_ts > current_time:
            return True
            
        # Проверяем грейс-период
        if grace_until > current_time:
            return True
            
        return False
    except Exception as e:
        logging.error(f"Ошибка проверки лицензии: {e}")
        return False

def get_local_status_record(user_id: int):
    """Возвращает локальный статус подписки.
    status: 'active' | 'grace' | 'none'
    payload: dict с полями expires_ts/grace_until/record
    """
    try:
        licenses = load_licenses()
        rec = licenses["users"].get(str(user_id))
        now = int(time.time())
        if rec:
            expires_ts = int(rec.get("expires_ts", 0) or 0)
            grace_until = int(rec.get("grace_until", 0) or 0)
            if expires_ts > now:
                return "active", {"expires_ts": expires_ts, "record": rec}
            if grace_until > now:
                return "grace", {"grace_until": grace_until, "record": rec}
        return "none", {"record": rec}
    except Exception as e:
        logging.error(f"get_local_status_record error: {e}")
        return "none", {}

def update_user_license(user_id, expires_ts, email=None):
    """Обновляет лицензию пользователя"""
    try:
        licenses = load_licenses()
        user_id_str = str(user_id)
        
        if user_id_str not in licenses["users"]:
            licenses["users"][user_id_str] = {}
            
        licenses["users"][user_id_str]["expires_ts"] = expires_ts
        licenses["users"][user_id_str].pop("grace_until", None)  # убираем грейс, если был
        logging.info(f"License updated for {user_id_str} until {expires_ts}")
        if email:
            licenses["users"][user_id_str]["email"] = email
            
        return save_licenses(licenses)
    except Exception as e:
        logging.error(f"Ошибка обновления лицензии: {e}")
        return False

def add_subscription_mapping(subscription_id, user_id):
    """Добавляет маппинг subscription_id -> user_id"""
    try:
        licenses = load_licenses()
        licenses["subs"][subscription_id] = {"user_id": int(user_id)}
        return save_licenses(licenses)
    except Exception as e:
        logging.error(f"Ошибка добавления маппинга подписки: {e}")
        return False

def get_user_by_subscription(subscription_id):
    """Получает user_id по subscription_id"""
    try:
        licenses = load_licenses()
        return licenses["subs"].get(subscription_id, {}).get("user_id")
    except Exception as e:
        logging.error(f"Ошибка получения пользователя по подписке: {e}")
        return None

# Безопасно получаем дату окончания периода подписки
def compute_expires_ts_from_subscription(subscription):
    """Возвращает timestamp окончания текущего периода подписки.
    Пытается прочитать subscription.current_period_end, затем берёт из последнего инвойса,
    и в крайнем случае возвращает +30 дней от текущего времени.
    """
    try:
        # 1) Прямая попытка (работает и для StripeObject, и для dict)
        expires_ts = None
        try:
            expires_ts = getattr(subscription, 'current_period_end', None)
        except Exception:
            expires_ts = None
        if not expires_ts and isinstance(subscription, dict):
            expires_ts = subscription.get('current_period_end')
        if expires_ts:
            return int(expires_ts)

        # 2) Пытаемся достать конец периода из последнего инвойса
        latest_invoice_id = None
        try:
            latest_invoice_id = getattr(subscription, 'latest_invoice', None)
        except Exception:
            latest_invoice_id = None
        if not latest_invoice_id and isinstance(subscription, dict):
            latest_invoice_id = subscription.get('latest_invoice')

        if latest_invoice_id:
            try:
                invoice = stripe.Invoice.retrieve(latest_invoice_id, expand=['lines.data'])
                # Берём первую позицию – это наша подписка
                lines = None
                try:
                    lines = getattr(invoice, 'lines', None)
                except Exception:
                    lines = invoice.get('lines') if isinstance(invoice, dict) else None
                data_list = None
                if lines:
                    try:
                        data_list = getattr(lines, 'data', None)
                    except Exception:
                        data_list = lines.get('data') if isinstance(lines, dict) else None
                if data_list and len(data_list) > 0:
                    period = data_list[0].get('period') if isinstance(data_list[0], dict) else getattr(data_list[0], 'period', None)
                    if period and ('end' in period):
                        return int(period['end'])
            except Exception as e:
                logging.warning(f"Failed to read period end from invoice {latest_invoice_id}: {e}")

        # 3) Фолбэк – даём 30 дней, чтобы юзер не зависал без доступа
        fallback = int(time.time()) + 30 * 24 * 60 * 60
        logging.warning("current_period_end not found; granting fallback 30 days")
        return fallback
    except Exception as e:
        logging.error(f"compute_expires_ts_from_subscription error: {e}")
        return int(time.time()) + 30 * 24 * 60 * 60

# Пытаемся восстановить лицензию напрямую из Stripe (после деплоя/перезапуска)
def recover_license_from_stripe(user_id: int) -> bool:
    try:
        # 1) Ищем подписку по metadata.user_id через Stripe Search API
        sub = None
        try:
            query = f"metadata['user_id']:'{user_id}' AND (status:'active' OR status:'trialing')"
            res = stripe.Subscription.search(query=query, limit=1)
            data = getattr(res, 'data', None) if hasattr(res, 'data') else res.get('data') if isinstance(res, dict) else None
            if data and len(data) > 0:
                sub = data[0]
        except Exception as e:
            logging.warning(f"Stripe search not available, fallback to list: {e}")
        # 2) Фолбэк: перебор последних подписок и фильтр по metadata
        if not sub:
            try:
                subs = stripe.Subscription.list(limit=50)
                for s in getattr(subs, 'data', subs.get('data', [])):
                    md = getattr(s, 'metadata', None)
                    if not md and isinstance(s, dict):
                        md = s.get('metadata')
                    if md and str(md.get('user_id')) == str(user_id) and getattr(s, 'status', s.get('status')) in ('active', 'trialing'):
                        sub = s
                        break
            except Exception as e:
                logging.warning(f"Stripe list fallback failed: {e}")
        if not sub:
            return False
        # 3) Вычисляем срок действия и сохраняем локально
        expires_ts = compute_expires_ts_from_subscription(sub)
        add_subscription_mapping(getattr(sub, 'id', sub.get('id')), user_id)
        update_user_license(int(user_id), expires_ts)
        logging.info(f"Recovered license from Stripe for user {user_id} until {expires_ts}")
        return True
    except Exception as e:
        logging.error(f"recover_license_from_stripe error: {e}")
        return False

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
    user_id = message.from_user.id
    
    # Проверяем лицензию; если нет — пробуем восстановить из Stripe (после деплоя)
    if not is_license_active(user_id):
        recovered = recover_license_from_stripe(user_id)
        if not recovered and not is_license_active(user_id):
            pay_url = f"{_base_url()}/pay/checkout?user_id={user_id}"
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="💳 Оплатить подписку", url=pay_url)]
            ])
            await message.reply(
                "Привет! Для использования бота нужна активная подписка.\n\n"
                "Нажмите кнопку ниже для оплаты:",
                reply_markup=keyboard
            )
            return
    
    await message.reply("Привет! Отправь мне ссылки на MEGA — я их скачаю и разархивирую.")
    await state.set_state(DownloadState.waiting_for_link)

# Разрешаем команды работать даже в состоянии ожидания ссылок
@dp.message(StateFilter(DownloadState.waiting_for_link), Command(commands=['status']))
async def status_in_waiting_state(message: types.Message, state: FSMContext):
    # Проксируем в общий обработчик статуса
    await status_command(message)

@dp.message(StateFilter(DownloadState.waiting_for_link), Command(commands=['start']))
async def start_in_waiting_state(message: types.Message, state: FSMContext):
    # Перезапускаем приветствие/проверку подписки и оставляем корректное состояние
    await send_welcome(message, state)

@dp.message(StateFilter(DownloadState.waiting_for_link), Command(commands=['cancel']))
async def cancel_in_waiting_state(message: types.Message, state: FSMContext):
    await state.clear()
    await message.reply("Режим ожидания ссылок сброшен. Отправьте /start или пришлите ссылку на MEGA.")

# обработка ссылок
@dp.message(StateFilter(DownloadState.waiting_for_link))
async def process_link(message: types.Message, state: FSMContext):
    import re

    # Если это другая команда (начинается с '/'), не пытаемся парсить как ссылку
    if message.text.strip().startswith('/'):
        await message.reply("Команда не распознана в режиме загрузки. Используйте /status, /cancel или /start.")
        return
    
    # Проверяем лицензию
    if not is_license_active(message.from_user.id):
        pay_url = f"{_base_url()}/pay/checkout?user_id={message.from_user.id}"
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить подписку", url=pay_url)]
        ])
        await message.reply(
            "Для обработки ссылок нужна активная подписка.\n\n"
            "Нажмите кнопку ниже для оплаты:",
            reply_markup=keyboard
        )
        return
    
    links = re.findall(r'https?://mega\.nz/\S+', message.text)
    if not links:
        await message.reply("Не найдено ни одной ссылки на MEGA.")
        return

    user_id = str(message.from_user.id)
    user_output_dir = Path(OUTPUT_DIR) / user_id
    user_output_dir.mkdir(parents=True, exist_ok=True)
    # Удаляем старые файлы перед началом новой загрузки
    import shutil
    for old_file in user_output_dir.glob("*"):
        if old_file.is_file():
            old_file.unlink()
        elif old_file.is_dir():
            shutil.rmtree(old_file, ignore_errors=True)
    import time
    start_time = time.time()

    for file in os.listdir(DOWNLOAD_DIR):
        file_path = Path(DOWNLOAD_DIR) / file
        if file_path.is_file():
            os.remove(file_path)

    for link in links:
        try:
            # Пробуем megatools, если недоступен - megadl
            if shutil.which("megatools"):
                cmd = ["megatools", "dl", "--path", str(DOWNLOAD_DIR), link]
            elif shutil.which("megadl"):
                cmd = ["megadl", "--path", str(DOWNLOAD_DIR), link]
            else:
                await message.reply("Ошибка: не найдены ни megatools, ни megadl")
                continue
                
            result = subprocess.run(cmd, capture_output=True, text=True)
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
    # Отфильтровываем только файлы, добавленные после начала обработки
    recent_files = []
    for f in final_files:
        if f.stat().st_mtime >= start_time:
            recent_files.append(f)
    final_files = recent_files

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

        remaining_seconds = 30 * 60
        minutes = remaining_seconds // 60
        seconds = remaining_seconds % 60
        countdown_str = f"{minutes:02}:{seconds:02}"

        download_link = f"https://{os.getenv('RAILWAY_STATIC_URL')}/download/{user_id}/{share_id}"
        archive_message = await message.reply(
            f"Готово! Вот ссылка для скачивания ZIP-архива:\n{download_link}\n\n"
            f"⏳ До удаления архива: {countdown_str}"
        )
        # Обновление сообщения каждые 30 секунд с обратным отсчётом
        async def update_countdown(msg, delay):
            for sec in range(delay - 1, 0, -30):  # Обновляем каждые 30 секунд
                minutes = sec // 60
                seconds = sec % 60
                countdown_str = f"{minutes:02}:{seconds:02}"
                try:
                    await msg.edit_text(
                        f"Готово! Вот ссылка для скачивания ZIP-архива:\n{download_link}\n\n"
                        f"⏳ До удаления архива: {countdown_str}"
                    )
                except Exception as e:
                    logging.warning(f"Не удалось обновить сообщение: {e}")
                    break
                await asyncio.sleep(30)  # Ждем 30 секунд между обновлениями

        asyncio.create_task(update_countdown(archive_message, remaining_seconds))
        buttons = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📥 Скачать снова", url=download_link)],
            [InlineKeyboardButton(text="🗑 Удалить архив", callback_data="delete_last")]
        ])
        await message.reply("Что хотите сделать дальше?", reply_markup=buttons)

# Команда /pay
@dp.message(Command(commands=['pay']))
async def pay_command(message: types.Message):
    if message.chat.type == 'private':
        if message.from_user.id == ADMIN_ID:
            await message.reply("Вы администратор: доступ открыт без подписки.")
            return
        pay_url = f"{_base_url()}/pay/checkout?user_id={message.from_user.id}"
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💳 Оплатить подписку", url=pay_url)]
        ])
        await message.reply(
            "Для использования бота нужна активная подписка.\n\n"
            "Нажмите кнопку ниже для оплаты:",
            reply_markup=keyboard
        )
    else:
        await message.reply("Подписка доступна только в личных сообщениях с ботом.")

# Команда /status
@dp.message(Command(commands=['status']))
async def status_command(message: types.Message):
    user_id = message.from_user.id

    # Админ — всегда активен
    try:
        if int(user_id) == int(ADMIN_ID):
            await message.reply("✅ У вас безлимитный доступ (администратор)")
            return
    except Exception:
        pass

    def _reply_active(ts: int):
        expires_date = datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M")
        return f"✅ Подписка активна до {expires_date}"

    # 1) Локальный статус
    status, payload = get_local_status_record(user_id)
    now = int(time.time())
    if status == "active":
        await message.reply(_reply_active(int(payload.get("expires_ts", now))))
        return
    if status == "grace":
        grace_date = datetime.fromtimestamp(int(payload.get("grace_until", now))).strftime("%d.%m.%Y %H:%M")
        await message.reply(f"⚠️ Грейс-период до {grace_date}")
        return

    # 2) Если локально нет записи, но доступ активен — подстрахуемся
    if is_license_active(user_id):
        if recover_license_from_stripe(user_id):
            status, payload = get_local_status_record(user_id)
            if status == "active":
                await message.reply(_reply_active(int(payload.get("expires_ts", now))))
                return
        await message.reply("✅ Подписка активна (инициализирую кэш, попробуйте /status ещё раз через минуту)")
        return

    # 3) Ленивое восстановление из Stripe
    if recover_license_from_stripe(user_id):
        status, payload = get_local_status_record(user_id)
        if status == "active":
            await message.reply(_reply_active(int(payload.get("expires_ts", now))))
            return

    # 4) Нет подписки
    rec = payload.get("record") if isinstance(payload, dict) else None
    if rec:
        await message.reply("❌ Подписка неактивна")
    else:
        await message.reply("❌ Подписка не найдена")

# Команда /link (fallback для ручного связывания по email)
@dp.message(Command(commands=['link']))
async def link_command(message: types.Message):
    try:
        # Извлекаем email из команды
        command_parts = message.text.split()
        if len(command_parts) < 2:
            await message.reply("Использование: /link <email>")
            return
            
        email = command_parts[1].strip()
        user_id = message.from_user.id
        
        licenses = load_licenses()
        
        # Проверяем, есть ли ожидающая оплата для этого email
        if email in licenses["pending_by_email"]:
            expires_ts = licenses["pending_by_email"][email]
            
            # Переносим в активные пользователи
            update_user_license(user_id, expires_ts, email)
            
            # Удаляем из ожидающих
            del licenses["pending_by_email"][email]
            save_licenses(licenses)
            
            expires_date = datetime.fromtimestamp(expires_ts).strftime("%d.%m.%Y %H:%M")
            await message.reply(f"✅ Лицензия активирована до {expires_date}")
        else:
            await message.reply("❌ Ожидающая оплата для этого email не найдена")
            
    except Exception as e:
        logging.error(f"Ошибка в команде /link: {e}")
        await message.reply("❌ Ошибка при обработке команды")

# Админские команды
@dp.message(Command(commands=['grant']))
async def grant_command(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("❌ Недостаточно прав")
        return
        
    try:
        command_parts = message.text.split()
        if len(command_parts) < 3:
            await message.reply("Использование: /grant <user_id> <days>")
            return
            
        target_user_id = int(command_parts[1])
        days = int(command_parts[2])
        
        expires_ts = int(time.time()) + (days * 24 * 60 * 60)
        update_user_license(target_user_id, expires_ts)
        
        expires_date = datetime.fromtimestamp(expires_ts).strftime("%d.%m.%Y %H:%M")
        await message.reply(f"✅ Лицензия выдана пользователю {target_user_id} до {expires_date}")
        
    except Exception as e:
        logging.error(f"Ошибка в команде /grant: {e}")
        await message.reply("❌ Ошибка при обработке команды")

@dp.message(Command(commands=['revoke']))
async def revoke_command(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.reply("❌ Недостаточно прав")
        return
        
    try:
        command_parts = message.text.split()
        if len(command_parts) < 2:
            await message.reply("Использование: /revoke <user_id>")
            return
            
        target_user_id = int(command_parts[1])
        
        licenses = load_licenses()
        user_id_str = str(target_user_id)
        
        if user_id_str in licenses["users"]:
            del licenses["users"][user_id_str]
            save_licenses(licenses)
            await message.reply(f"✅ Лицензия пользователя {target_user_id} отозвана")
        else:
            await message.reply(f"❌ Пользователь {target_user_id} не найден")
            
    except Exception as e:
        logging.error(f"Ошибка в команде /revoke: {e}")
        await message.reply("❌ Ошибка при обработке команды")

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
    try:
        logging.info(f"Licenses file exists: {os.path.exists(LICENSES_FILE)} at {LICENSES_FILE}")
    except Exception:
        pass
    # Обработчик для скачивания файлов
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

    # Обработчик для health check
    async def handle_health(request):
        return web.Response(text="ok")

    # Обработчик для создания Stripe Checkout Session
    async def handle_checkout(request):
        try:
            user_id = request.query.get('user_id')
            if not user_id or not user_id.isdigit() or int(user_id) <= 0:
                return web.Response(status=400, text="Invalid user_id")
            
            user_id = int(user_id)
            
            # Создаём параметры для Checkout Session
            session_kwargs = dict(
                mode='subscription',
                line_items=[{
                    'price': STRIPE_PRICE_ID,
                    'quantity': 1,
                }],
                metadata={
                    'user_id': str(user_id)
                },
                subscription_data={
                    'metadata': {'user_id': str(user_id)},
                },
                success_url=f"{_base_url()}/pay/success?u={user_id}",
                cancel_url=f"{_base_url()}/pay/cancel?u={user_id}",
            )
            # Добавляем триал, если указан в переменных окружения
            if STRIPE_TRIAL_DAYS > 0:
                session_kwargs['subscription_data']['trial_period_days'] = STRIPE_TRIAL_DAYS

            checkout_session = stripe.checkout.Session.create(**session_kwargs)
            
            logging.info(f"Created checkout session for user {user_id}: {checkout_session.id}")
            return web.HTTPFound(location=checkout_session.url)
            
        except Exception as e:
            logging.error(f"Error creating checkout session: {e}")
            return web.Response(status=500, text="Internal server error")

    # Обработчик для Stripe webhooks
    async def handle_stripe_webhook(request):
        try:
            payload = await request.text()
            sig_header = request.headers.get('stripe-signature')
            
            # Верифицируем подпись
            try:
                event = stripe.Webhook.construct_event(
                    payload, sig_header, STRIPE_WEBHOOK_SECRET
                )
            except ValueError as e:
                logging.error(f"Invalid payload: {e}")
                return web.Response(status=400, text="Invalid payload")
            except stripe.error.SignatureVerificationError as e:
                logging.error(f"Invalid signature: {e}")
                return web.Response(status=400, text="Invalid signature")
            
            # Обрабатываем события
            if event['type'] == 'checkout.session.completed':
                session = event['data']['object']
                metadata = session.get('metadata') or {}
                user_id = metadata.get('user_id')
                subscription_id = session.get('subscription')

                if subscription_id:
                    # Получаем информацию о подписке
                    subscription = stripe.Subscription.retrieve(subscription_id)
                    expires_ts = compute_expires_ts_from_subscription(subscription)

                    if user_id:
                        # Сохраняем маппинг subscription -> user_id
                        add_subscription_mapping(subscription_id, user_id)
                        # Обновляем лицензию пользователя
                        update_user_license(int(user_id), expires_ts)
                        logging.info(f"Subscription activated for user {user_id} until {expires_ts}")
                    else:
                        # user_id нет (например, оплата не через бот-чекаут). Привязываем по email через /link
                        email = None
                        # пытаемся взять email из session
                        customer_details = session.get('customer_details') or {}
                        email = customer_details.get('email')
                        # если нет — пробуем достать из Customer
                        if not email:
                            cust_id = session.get('customer')
                            if cust_id:
                                try:
                                    cust = stripe.Customer.retrieve(cust_id)
                                    email = (cust.get('email') or '').strip() if isinstance(cust, dict) else None
                                except Exception as e:  # не фейлим обработку
                                    logging.warning(f"Unable to retrieve customer {cust_id}: {e}")
                        if email:
                            licenses = load_licenses()
                            licenses.setdefault('pending_by_email', {})
                            licenses['pending_by_email'][email] = expires_ts
                            save_licenses(licenses)
                            logging.info(f"Stored pending license by email {email} until {expires_ts}; user can run /link {email}")
                        else:
                            logging.warning("checkout.session.completed without user_id and email – cannot link automatically")

            elif event['type'] == 'invoice.payment_succeeded':
                invoice = event['data']['object']
                subscription_id = invoice.get('subscription')
                
                if subscription_id:
                    # Находим пользователя по subscription_id
                    user_id = get_user_by_subscription(subscription_id)

                    # Если маппинга ещё нет (например, оплата через Payment Link без metadata) — пробуем связать по email
                    if not user_id:
                        email = (invoice.get('customer_email') or '').strip()
                        if not email:
                            cust_id = invoice.get('customer')
                            if cust_id:
                                try:
                                    cust = stripe.Customer.retrieve(cust_id)
                                    email = (cust.get('email') or '').strip() if isinstance(cust, dict) else None
                                except Exception as e:
                                    logging.warning(f"Unable to retrieve customer {cust_id}: {e}")
                        if email:
                            # если ранее мы сохранили pending_by_email, активируем по команде /link; здесь просто кэшируем срок
                            try:
                                subscription = stripe.Subscription.retrieve(subscription_id)
                                expires_ts = compute_expires_ts_from_subscription(subscription)
                                licenses = load_licenses()
                                licenses.setdefault('pending_by_email', {})
                                licenses['pending_by_email'][email] = expires_ts
                                save_licenses(licenses)
                                logging.info(f"Stored pending license by email {email} until {expires_ts} (awaiting /link {email})")
                            except Exception as e:
                                logging.warning(f"Failed to cache pending license for {email}: {e}")

                    if user_id:
                        # Получаем обновленную информацию о подписке
                        subscription = stripe.Subscription.retrieve(subscription_id)
                        expires_ts = compute_expires_ts_from_subscription(subscription)
                        # Обновляем лицензию
                        update_user_license(user_id, expires_ts)
                        logging.info(f"Subscription renewed for user {user_id} until {expires_ts}")
            
            elif event['type'] == 'invoice.payment_failed':
                invoice = event['data']['object']
                subscription_id = invoice.get('subscription')
                
                if subscription_id:
                    user_id = get_user_by_subscription(subscription_id)
                    
                    if user_id:
                        # Устанавливаем грейс-период на 3 дня
                        grace_until = int(time.time()) + (3 * 24 * 60 * 60)
                        
                        licenses = load_licenses()
                        user_id_str = str(user_id)
                        if user_id_str in licenses["users"]:
                            licenses["users"][user_id_str]["grace_until"] = grace_until
                            save_licenses(licenses)
                            
                        logging.info(f"Payment failed for user {user_id}, grace period until {grace_until}")
            
            elif event['type'] == 'customer.subscription.deleted':
                subscription = event['data']['object']
                subscription_id = subscription['id']
                
                user_id = get_user_by_subscription(subscription_id)
                
                if user_id:
                    # Удаляем лицензию пользователя
                    licenses = load_licenses()
                    user_id_str = str(user_id)
                    if user_id_str in licenses["users"]:
                        del licenses["users"][user_id_str]
                    if subscription_id in licenses["subs"]:
                        del licenses["subs"][subscription_id]
                    save_licenses(licenses)
                    
                    logging.info(f"Subscription cancelled for user {user_id}")
            
            return web.Response(text="OK")
            
        except Exception as e:
            logging.error(f"Error processing webhook: {e}")
            return web.Response(status=500, text="Internal server error")

    # Создаем aiohttp приложение
    app = web.Application()
    app.router.add_get("/health", handle_health)
    app.router.add_get("/pay/checkout", handle_checkout)
    # Stripe webhook: add both no-slash and slash aliases + optional GET for diagnostics
    app.router.add_post("/webhooks/stripe", handle_stripe_webhook)
    app.router.add_post("/webhooks/stripe/", handle_stripe_webhook)
    app.router.add_get("/webhooks/stripe", handle_health)  # returns 200 on GET for quick checks
    app.router.add_get("/webhooks/stripe/", handle_health)
    app.router.add_get("/download/{token1}/{token2}", handle_download)

    # Success/Cancel landing pages to avoid 404 after checkout
    async def handle_pay_success(request):
        return web.Response(text="Payment step finished. You can return to Telegram.")
    async def handle_pay_cancel(request):
        return web.Response(text="Payment was cancelled. You can return to Telegram and try again.")
    app.router.add_get("/pay/success", handle_pay_success)
    app.router.add_get("/pay/cancel", handle_pay_cancel)

    # Root route for sanity (helps avoid 502 for GET /)
    async def handle_root(request):
        return web.Response(text="ok")
    app.router.add_get("/", handle_root)


    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", "8080"))
    site = web.TCPSite(runner, port=port)
    await site.start()

    await bot.delete_webhook(drop_pending_updates=True)
    # Start Telegram long-polling in background (single instance on Railway)
    asyncio.create_task(dp.start_polling(bot))

    # Stay alive
    await asyncio.Event().wait()


# --- Entrypoint ---
if __name__ == "__main__":
    asyncio.run(main())
