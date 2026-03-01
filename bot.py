import asyncio
import json
import logging
import os
import random
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import KeyboardButton, Message, ReplyKeyboardMarkup


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_FILE = Path(os.getenv("DATA_FILE", "data/schedules.json"))
DEFAULT_FORMAT = "text"

BTN_SETUP = "⚙️ Настроить"
BTN_STATUS = "📊 Статус"
BTN_STOP = "⏹ Остановить"
BTN_EDIT = "✏️ Изменить настройки"
BTN_HELP = "❓ Помощь"


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_SETUP), KeyboardButton(text=BTN_EDIT)],
            [KeyboardButton(text=BTN_STATUS), KeyboardButton(text=BTN_STOP)],
            [KeyboardButton(text=BTN_HELP)],
        ],
        resize_keyboard=True,
    )


def choice_keyboard(options: list[str]) -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=option)] for option in options],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


class SetupStates(StatesGroup):
    start_time = State()
    end_time = State()
    count = State()
    message_mode = State()
    messages = State()
    day_filter = State()
    repeat_daily = State()
    notify_format = State()


@dataclass
class UserSchedule:
    start_time: str
    end_time: str
    count: int
    messages: list[str]
    day_filter: str
    repeat_daily: bool
    notify_format: str


class JsonStorage:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self.data: dict[str, Any] = {"users": {}}

    async def load(self) -> None:
        async with self._lock:
            if self.path.exists():
                self.data = json.loads(self.path.read_text(encoding="utf-8"))
            else:
                self.path.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")

    async def save(self) -> None:
        async with self._lock:
            self.path.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8")

    def get_user(self, user_id: int) -> dict[str, Any]:
        users = self.data.setdefault("users", {})
        return users.setdefault(str(user_id), {"active": False, "config": None, "notifications": []})


def parse_time(text: str) -> time:
    return datetime.strptime(text.strip(), "%H:%M").time()


def should_run_today(day_filter: str, dt: datetime) -> bool:
    weekday = dt.weekday()
    if day_filter == "weekdays":
        return weekday < 5
    if day_filter == "weekends":
        return weekday >= 5
    return True


def apply_format(base_text: str, notify_format: str) -> str:
    if notify_format == "emoji":
        emojis = ["✨", "🔥", "💡", "🚀", "🌟"]
        return f"{random.choice(emojis)} {base_text} {random.choice(emojis)}"
    if notify_format == "quote":
        quotes = [
            "Маленькие шаги каждый день дают большой результат.",
            "Сфокусируйся на процессе, и результат придёт.",
            "Сделай это сейчас, а не когда-нибудь потом.",
        ]
        return f"{base_text}\n\n📌 {random.choice(quotes)}"
    if notify_format == "number":
        return f"{base_text}\nСлучайное число дня: {random.randint(1, 100)}"
    return base_text


class NotificationService:
    def __init__(self, bot: Bot, storage: JsonStorage):
        self.bot = bot
        self.storage = storage
        self.running_tasks: dict[tuple[int, str], asyncio.Task] = {}
        self.monitor_task: asyncio.Task | None = None

    async def start(self) -> None:
        self.monitor_task = asyncio.create_task(self.monitor())

    async def stop(self) -> None:
        if self.monitor_task:
            self.monitor_task.cancel()
        for task in self.running_tasks.values():
            task.cancel()
        self.running_tasks.clear()

    async def monitor(self) -> None:
        while True:
            try:
                await self.restore_and_schedule()
                await asyncio.sleep(20)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Scheduler monitor error: %s", exc)
                await asyncio.sleep(5)

    async def restore_and_schedule(self) -> None:
        users = self.storage.data.get("users", {})
        now = datetime.now()

        for user_id_str, user_data in users.items():
            user_id = int(user_id_str)
            if not user_data.get("active"):
                continue

            config = user_data.get("config")
            notifications = user_data.get("notifications", [])

            if config and config.get("repeat_daily") and notifications and all(n.get("sent") for n in notifications):
                await self.generate_schedule(user_id, UserSchedule(**config))
                user_data = self.storage.get_user(user_id)
                notifications = user_data.get("notifications", [])

            for notification in notifications:
                if notification.get("sent"):
                    continue

                key = (user_id, notification["id"])
                if key in self.running_tasks:
                    continue

                send_at = datetime.fromisoformat(notification["send_at"])
                if send_at < now - timedelta(minutes=5):
                    notification["sent"] = True
                    await self.storage.save()
                    continue

                self.running_tasks[key] = asyncio.create_task(self._deliver_notification(user_id, notification))

    async def _deliver_notification(self, user_id: int, notification: dict[str, Any]) -> None:
        key = (user_id, notification["id"])
        try:
            send_at = datetime.fromisoformat(notification["send_at"])
            jitter_seconds = random.randint(-180, 180)
            actual_time = send_at + timedelta(seconds=jitter_seconds)
            sleep_seconds = (actual_time - datetime.now()).total_seconds()
            if sleep_seconds > 0:
                await asyncio.sleep(sleep_seconds)

            user_data = self.storage.get_user(user_id)
            config = user_data.get("config") or {}
            text = apply_format(notification["text"], config.get("notify_format", DEFAULT_FORMAT))
            await self.bot.send_message(chat_id=user_id, text=f"🔔 {text}", parse_mode=ParseMode.HTML)

            for n in user_data.get("notifications", []):
                if n["id"] == notification["id"]:
                    n["sent"] = True
                    break
            await self.storage.save()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Error while delivering notification to %s: %s", user_id, exc)
        finally:
            self.running_tasks.pop(key, None)

    async def generate_schedule(self, user_id: int, config: UserSchedule) -> list[dict[str, Any]]:
        now = datetime.now()
        start_t = parse_time(config.start_time)
        end_t = parse_time(config.end_time)

        target_date = None
        window_start_dt = None
        end_dt = None

        for day_shift in range(8):
            candidate_date = now.date() + timedelta(days=day_shift)
            candidate_start = datetime.combine(candidate_date, start_t)
            candidate_end = datetime.combine(candidate_date, end_t)

            if candidate_end <= candidate_start:
                raise ValueError("Время окончания должно быть больше времени начала.")

            if not should_run_today(config.day_filter, candidate_start):
                continue

            if candidate_end <= now:
                continue

            target_date = candidate_date
            window_start_dt = max(candidate_start, now)
            end_dt = candidate_end
            break

        if target_date is None or window_start_dt is None or end_dt is None:
            raise ValueError("Не удалось подобрать подходящий день для отправок. Проверьте настройки.")

        interval = int((end_dt - window_start_dt).total_seconds())
        if interval <= 0:
            raise ValueError("В выбранном диапазоне уже не осталось времени для отправок.")
        if config.count > interval:
            raise ValueError("Слишком много уведомлений для оставшегося диапазона времени.")

        random_points = sorted(random.sample(range(interval), config.count))
        notifications: list[dict[str, Any]] = []
        for i, point in enumerate(random_points, start=1):
            notifications.append(
                {
                    "id": f"{target_date.isoformat()}-{i}",
                    "send_at": (window_start_dt + timedelta(seconds=point)).isoformat(),
                    "text": random.choice(config.messages),
                    "sent": False,
                }
            )

        user_data = self.storage.get_user(user_id)
        user_data["active"] = True
        user_data["config"] = config.__dict__
        user_data["notifications"] = notifications
        await self.storage.save()
        return notifications


def build_status(user_data: dict[str, Any]) -> str:
    if not user_data.get("active"):
        return "Сейчас уведомления остановлены. Используйте /start или кнопку «⚙️ Настроить»."

    config = user_data.get("config") or {}
    notifications = user_data.get("notifications", [])
    pending = [n for n in notifications if not n.get("sent")]

    lines = [
        "📊 <b>Текущий статус</b>",
        f"Период: {config.get('start_time')} - {config.get('end_time')}",
        f"Количество: {config.get('count')}",
        f"Фильтр дней: {config.get('day_filter')}",
        f"Повтор ежедневно: {'да' if config.get('repeat_daily') else 'нет'}",
        f"Формат: {config.get('notify_format')}",
        f"Осталось уведомлений: {len(pending)}",
        "",
        "Ближайшие отправки:",
    ]

    for n in sorted(pending, key=lambda item: item["send_at"])[:10]:
        dt = datetime.fromisoformat(n["send_at"]).strftime("%d.%m %H:%M")
        lines.append(f"• {dt} — {n['text']}")

    if not pending:
        lines.append("• Нет активных отправок")

    return "\n".join(lines)


def _is_keep_value(raw: str, edit_mode: bool) -> bool:
    return edit_mode and raw == "-"


async def main() -> None:
    token = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError(
            "Не найден токен бота. Установите переменную окружения BOT_TOKEN "
            "(или TELEGRAM_BOT_TOKEN) в Render/Railway → Variables и сделайте redeploy."
        )

    bot = Bot(token=token)
    dp = Dispatcher(storage=MemoryStorage())

    storage = JsonStorage(DATA_FILE)
    await storage.load()
    service = NotificationService(bot, storage)
    await service.start()

    async def begin_setup(message: Message, state: FSMContext, edit_mode: bool = False) -> None:
        await state.clear()
        if edit_mode:
            user_data = storage.get_user(message.from_user.id)
            config = user_data.get("config")
            if not config:
                await message.answer("У вас пока нет настроек. Давайте создадим новые.", reply_markup=main_menu_keyboard())
                edit_mode = False
            else:
                await state.update_data(existing_config=config)

        await state.update_data(edit_mode=edit_mode)
        prompt = (
            "Шаг 1/8: Введите время начала в формате HH:MM (например, 14:00)."
            if not edit_mode
            else "Шаг 1/8: Введите новое время начала HH:MM или '-' чтобы оставить текущее."
        )
        await message.answer(prompt, reply_markup=main_menu_keyboard())
        await state.set_state(SetupStates.start_time)

    @dp.message(Command("start"))
    async def cmd_start(message: Message, state: FSMContext) -> None:
        await state.clear()
        await message.answer(
            "Привет! Я помогу настроить случайные уведомления.\n"
            "Выберите действие кнопкой ниже или используйте команды /setup /status /edit /stop.",
            reply_markup=main_menu_keyboard(),
        )

    @dp.message(Command("setup"))
    async def cmd_setup(message: Message, state: FSMContext) -> None:
        await begin_setup(message, state, edit_mode=False)

    @dp.message(Command("edit"))
    async def cmd_edit(message: Message, state: FSMContext) -> None:
        await begin_setup(message, state, edit_mode=True)

    @dp.message(lambda m: (m.text or "").strip() == BTN_SETUP)
    async def btn_setup(message: Message, state: FSMContext) -> None:
        await begin_setup(message, state, edit_mode=False)

    @dp.message(lambda m: (m.text or "").strip() == BTN_EDIT)
    async def btn_edit(message: Message, state: FSMContext) -> None:
        await begin_setup(message, state, edit_mode=True)

    @dp.message(lambda m: (m.text or "").strip() == BTN_STATUS)
    async def btn_status(message: Message) -> None:
        user_data = storage.get_user(message.from_user.id)
        await message.answer(build_status(user_data), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

    @dp.message(lambda m: (m.text or "").strip() == BTN_STOP)
    async def btn_stop(message: Message) -> None:
        await cmd_stop(message)

    @dp.message(lambda m: (m.text or "").strip() == BTN_HELP)
    async def btn_help(message: Message) -> None:
        await cmd_help(message)

    @dp.message(SetupStates.start_time)
    async def setup_start_time(message: Message, state: FSMContext) -> None:
        raw = (message.text or "").strip()
        data = await state.get_data()
        edit_mode = data.get("edit_mode", False)
        existing = data.get("existing_config") or {}

        if _is_keep_value(raw, edit_mode):
            start_value = existing.get("start_time")
            if not start_value:
                await message.answer("Нет предыдущего значения. Введите время HH:MM.")
                return
        else:
            try:
                parse_time(raw)
            except ValueError:
                await message.answer("Некорректный формат. Введите время начала HH:MM.")
                return
            start_value = raw

        await state.update_data(start_time=start_value)
        await message.answer(
            "Шаг 2/8: Введите время окончания HH:MM" + (" или '-' чтобы оставить текущее." if edit_mode else ".")
        )
        await state.set_state(SetupStates.end_time)

    @dp.message(SetupStates.end_time)
    async def setup_end_time(message: Message, state: FSMContext) -> None:
        raw = (message.text or "").strip()
        data = await state.get_data()
        edit_mode = data.get("edit_mode", False)
        existing = data.get("existing_config") or {}

        if _is_keep_value(raw, edit_mode):
            end_value = existing.get("end_time")
            if not end_value:
                await message.answer("Нет предыдущего значения. Введите время HH:MM.")
                return
        else:
            try:
                parse_time(raw)
            except ValueError:
                await message.answer("Некорректный формат. Введите время окончания HH:MM.")
                return
            end_value = raw

        await state.update_data(end_time=end_value)
        await message.answer("Шаг 3/8: Сколько уведомлений отправить?" + (" (или '-' чтобы оставить текущее)" if edit_mode else ""))
        await state.set_state(SetupStates.count)

    @dp.message(SetupStates.count)
    async def setup_count(message: Message, state: FSMContext) -> None:
        raw = (message.text or "").strip()
        data = await state.get_data()
        edit_mode = data.get("edit_mode", False)
        existing = data.get("existing_config") or {}

        if _is_keep_value(raw, edit_mode):
            count_value = existing.get("count")
            if not count_value:
                await message.answer("Нет предыдущего значения. Введите число, например 5.")
                return
        else:
            if not raw.isdigit() or int(raw) <= 0:
                await message.answer("Введите положительное число (например, 5).")
                return
            count_value = int(raw)

        await state.update_data(count=count_value)
        await message.answer(
            "Шаг 4/8: Выберите режим текста:\n"
            "• Один текст\n• Список текстов\n"
            + ("• '-' оставить текущие тексты" if edit_mode else ""),
            reply_markup=choice_keyboard(["Один текст", "Список текстов"] + (["-"] if edit_mode else [])),
        )
        await state.set_state(SetupStates.message_mode)

    @dp.message(SetupStates.message_mode)
    async def setup_message_mode(message: Message, state: FSMContext) -> None:
        raw = (message.text or "").strip()
        data = await state.get_data()
        edit_mode = data.get("edit_mode", False)
        existing = data.get("existing_config") or {}

        if _is_keep_value(raw, edit_mode):
            current_messages = existing.get("messages") or []
            if not current_messages:
                await message.answer("Нет предыдущих сообщений. Выберите режим: Один текст или Список текстов.")
                return
            await state.update_data(messages=current_messages, message_mode="keep")
            await message.answer(
                "Шаг 6/8: Когда отправлять?",
                reply_markup=choice_keyboard(["all", "weekdays", "weekends"] + (["-"] if edit_mode else [])),
            )
            await state.set_state(SetupStates.day_filter)
            return

        if raw not in {"Один текст", "Список текстов"}:
            await message.answer("Выберите кнопку: Один текст / Список текстов.")
            return

        await state.update_data(message_mode="single" if raw == "Один текст" else "list")
        await message.answer(
            "Шаг 5/8: Введите текст уведомления."
            if raw == "Один текст"
            else "Шаг 5/8: Введите тексты через ';' (например: Пей воду;Разомнись)."
        )
        await state.set_state(SetupStates.messages)

    @dp.message(SetupStates.messages)
    async def setup_messages(message: Message, state: FSMContext) -> None:
        raw = (message.text or "").strip()
        if not raw:
            await message.answer("Сообщение не должно быть пустым.")
            return

        data = await state.get_data()
        mode = data.get("message_mode")

        messages = [raw] if mode == "single" else [m.strip() for m in raw.split(";") if m.strip()]
        if not messages:
            await message.answer("Нужно хотя бы одно сообщение.")
            return

        await state.update_data(messages=messages)
        await message.answer(
            "Шаг 6/8: Когда отправлять?",
            reply_markup=choice_keyboard(["all", "weekdays", "weekends"] + (["-"] if data.get("edit_mode") else [])),
        )
        await state.set_state(SetupStates.day_filter)

    @dp.message(SetupStates.day_filter)
    async def setup_day_filter(message: Message, state: FSMContext) -> None:
        raw = (message.text or "").strip().lower()
        data = await state.get_data()
        edit_mode = data.get("edit_mode", False)
        existing = data.get("existing_config") or {}

        if _is_keep_value(raw, edit_mode):
            day_filter = existing.get("day_filter")
            if not day_filter:
                await message.answer("Нет предыдущего значения. Выберите all/weekdays/weekends.")
                return
        else:
            if raw not in {"all", "weekdays", "weekends"}:
                await message.answer("Выберите all, weekdays или weekends.")
                return
            day_filter = raw

        await state.update_data(day_filter=day_filter)
        await message.answer(
            "Шаг 7/8: Повторять ежедневно?",
            reply_markup=choice_keyboard(["yes", "no"] + (["-"] if edit_mode else [])),
        )
        await state.set_state(SetupStates.repeat_daily)

    @dp.message(SetupStates.repeat_daily)
    async def setup_repeat_daily(message: Message, state: FSMContext) -> None:
        raw = (message.text or "").strip().lower()
        data = await state.get_data()
        edit_mode = data.get("edit_mode", False)
        existing = data.get("existing_config") or {}

        if _is_keep_value(raw, edit_mode):
            repeat_daily = existing.get("repeat_daily")
            if repeat_daily is None:
                await message.answer("Нет предыдущего значения. Выберите yes или no.")
                return
        else:
            if raw not in {"yes", "no"}:
                await message.answer("Выберите yes или no.")
                return
            repeat_daily = raw == "yes"

        await state.update_data(repeat_daily=repeat_daily)
        await message.answer(
            "Шаг 8/8: Формат уведомлений.",
            reply_markup=choice_keyboard(["text", "emoji", "quote", "number"] + (["-"] if edit_mode else [])),
        )
        await state.set_state(SetupStates.notify_format)

    @dp.message(SetupStates.notify_format)
    async def setup_notify_format(message: Message, state: FSMContext) -> None:
        raw = (message.text or "").strip().lower()
        data = await state.get_data()
        edit_mode = data.get("edit_mode", False)
        existing = data.get("existing_config") or {}

        if _is_keep_value(raw, edit_mode):
            notify_format = existing.get("notify_format")
            if not notify_format:
                await message.answer("Нет предыдущего значения. Выберите text/emoji/quote/number.")
                return
        else:
            if raw not in {"text", "emoji", "quote", "number"}:
                await message.answer("Выберите text, emoji, quote или number.")
                return
            notify_format = raw

        merged_messages = data.get("messages")
        if not merged_messages:
            merged_messages = (existing.get("messages") if existing else None) or ["Напоминание!"]

        cfg = UserSchedule(
            start_time=data["start_time"],
            end_time=data["end_time"],
            count=int(data["count"]),
            messages=merged_messages,
            day_filter=data["day_filter"],
            repeat_daily=bool(data["repeat_daily"]),
            notify_format=notify_format,
        )

        try:
            notifications = await service.generate_schedule(message.from_user.id, cfg)
        except ValueError as exc:
            await message.answer(f"Ошибка настройки: {exc}\nНажмите «⚙️ Настроить» и попробуйте снова.", reply_markup=main_menu_keyboard())
            await state.clear()
            return

        lines = [
            "✅ Настройки сохранены!" if edit_mode else "✅ Расписание создано!",
            f"Запланировано уведомлений: {len(notifications)}",
        ]
        for n in notifications[:10]:
            dt = datetime.fromisoformat(n["send_at"]).strftime("%d.%m %H:%M")
            lines.append(f"• {dt} — {n['text']}")

        await message.answer("\n".join(lines), reply_markup=main_menu_keyboard())
        await state.clear()

    @dp.message(Command("status"))
    async def cmd_status(message: Message) -> None:
        user_data = storage.get_user(message.from_user.id)
        await message.answer(build_status(user_data), parse_mode=ParseMode.HTML, reply_markup=main_menu_keyboard())

    @dp.message(Command("stop"))
    async def cmd_stop(message: Message) -> None:
        user_id = message.from_user.id
        user_data = storage.get_user(user_id)
        user_data["active"] = False
        user_data["notifications"] = []
        await storage.save()

        for key, task in list(service.running_tasks.items()):
            if key[0] == user_id:
                task.cancel()
                service.running_tasks.pop(key, None)

        await message.answer("⏹ Уведомления остановлены. Чтобы настроить снова, нажмите «⚙️ Настроить».", reply_markup=main_menu_keyboard())

    @dp.message(Command("help"))
    async def cmd_help(message: Message) -> None:
        await message.answer(
            "Доступные команды и кнопки:\n"
            "/start — открыть главное меню\n"
            "/setup — новая настройка расписания\n"
            "/edit — изменить текущие настройки (можно писать '-' для пропуска)\n"
            "/status — текущее расписание\n"
            "/stop — остановить уведомления\n"
            "\nТакже можно просто нажимать кнопки внизу экрана.",
            reply_markup=main_menu_keyboard(),
        )

    @dp.message()
    async def fallback(message: Message) -> None:
        await message.answer(
            "Не понял команду. Нажмите кнопку внизу или используйте /help.",
            reply_markup=main_menu_keyboard(),
        )

    try:
        await dp.start_polling(bot)
    finally:
        await service.stop()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
