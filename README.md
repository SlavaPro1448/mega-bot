# Razarhivator - Telegram Bot с Stripe подпиской

Telegram-бот для скачивания и разархивирования файлов с MEGA с платной подпиской через Stripe.

## Возможности

- Скачивание файлов с MEGA (поддержка megatools и megadl)
- Рекурсивная распаковка архивов (ZIP, RAR, 7Z)
- Фильтрация только .json и .session файлов
- Временная выдача ZIP-архивов через HTTP
- Автоматическое удаление через 30 минут
- Платная подписка через Stripe с автопродлением
- Грейс-период при неуспешной оплате
- Админские команды для управления лицензиями

## Настройка Stripe

1. Создайте аккаунт в [Stripe Dashboard](https://dashboard.stripe.com)
2. Создайте Product с Recurring Price (ежемесячная подписка)
3. Получите `STRIPE_PRICE_ID` из созданного Price
4. Получите `STRIPE_SECRET_KEY` из API Keys
5. Создайте Webhook endpoint:
   - URL: `https://your-domain.com/webhooks/stripe`
   - События: `checkout.session.completed`, `invoice.payment_succeeded`, `invoice.payment_failed`, `customer.subscription.deleted`
6. Получите `STRIPE_WEBHOOK_SECRET` из webhook

## Переменные окружения

```bash
BOT_TOKEN=your_telegram_bot_token
PUBLIC_BASE_URL=https://your-domain.com
STRIPE_SECRET_KEY=sk_test_...
STRIPE_PRICE_ID=price_...
STRIPE_WEBHOOK_SECRET=whsec_...
ADMIN_ID=your_telegram_user_id
MAX_UNPACK_BYTES=0  # 0 = без ограничений
```

## Установка и запуск

1. Установите зависимости:
```bash
pip install -r requirements.txt
```

2. Установите megatools или megadl:
```bash
# Ubuntu/Debian
sudo apt-get install megatools

# Или используйте megadl
```

3. Запустите бота:
```bash
python razarhivator.py
```

## Команды бота

- `/start` - Начать работу (проверяет подписку)
- `/pay` - Оплатить подписку
- `/status` - Проверить статус подписки
- `/link <email>` - Связать аккаунт по email (fallback)

### Админские команды

- `/grant <user_id> <days>` - Выдать лицензию на N дней
- `/revoke <user_id>` - Отозвать лицензию

## API Endpoints

- `GET /health` - Health check
- `GET /pay/checkout?user_id=<id>` - Создание Stripe Checkout Session
- `POST /webhooks/stripe` - Обработка Stripe webhooks
- `GET /download/{token1}/{token2}` - Скачивание ZIP-архивов

## Структура данных

Лицензии хранятся в `/app/licenses.json`:

```json
{
  "users": {
    "123456789": {
      "expires_ts": 1731000000,
      "email": "user@example.com"
    }
  },
  "pending_by_email": {
    "user@example.com": 1731000000
  },
  "subs": {
    "sub_1234567890": {
      "user_id": 123456789
    }
  }
}
```

## Логика работы

1. Пользователь отправляет `/start` или ссылки на MEGA
2. Бот проверяет активность подписки
3. Если подписки нет - предлагает оплату через Stripe
4. После успешной оплаты Stripe отправляет webhook
5. Бот активирует подписку и разрешает обработку ссылок
6. При продлении/отмене подписки webhook обновляет статус
7. При неуспешной оплате устанавливается грейс-период на 3 дня
