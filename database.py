import sqlite3

from pathlib import Path
import shutil
from typing import List, Tuple, Optional

import time

from services.user_paths import (
    BASE_DIR,
    broadcast_config_path,
    broadcast_config_backup_path,
    user_broadcast_dir,
)


# Путь к базе данных

OLD_DB_PATH = Path(__file__).parent / "users.db"
DB_PATH = BASE_DIR / "_common" / "users.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)





def _retry_db_operation(func, max_retries=3, base_delay=0.5):

    """Выполнить DB операцию с retry при database locked"""

    for attempt in range(max_retries):

        try:

            return func()

        except sqlite3.OperationalError as e:

            if 'database is locked' in str(e) and attempt < max_retries - 1:

                delay = base_delay * (2 ** attempt)

                time.sleep(delay)

                continue

            raise





def init_db():

    """Инициализация базы данных"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    # Включаем WAL mode для лучшей параллельности

    cursor.execute("PRAGMA journal_mode=WAL")

    # Увеличиваем cache_size для лучшей производительности

    cursor.execute("PRAGMA cache_size=-64000")  # 64MB

    # Синхронизация на нормальный уровень (не fsync на каждую операцию)

    cursor.execute("PRAGMA synchronous=NORMAL")

    

    # Таблица пользователей

    cursor.execute("""

        CREATE TABLE IF NOT EXISTS users (

            user_id INTEGER PRIMARY KEY,

            username TEXT,

            first_name TEXT,

            is_logged_in BOOLEAN DEFAULT 0

        )

    """)

    

    # Таблица сессий логина

    cursor.execute("""

        CREATE TABLE IF NOT EXISTS login_sessions (

            user_id INTEGER PRIMARY KEY,

            phone_number TEXT,

            step TEXT DEFAULT 'phone'

        )

    """)

    

    # Таблица отслеживаемых чатов

    cursor.execute("""

        CREATE TABLE IF NOT EXISTS tracked_chats (

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            user_id INTEGER,

            chat_id INTEGER,

            chat_name TEXT,

            UNIQUE(user_id, chat_id)

        )

    """)

    

    # Таблица чатов для рассылки

    cursor.execute("""

        CREATE TABLE IF NOT EXISTS broadcast_chats (

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            user_id INTEGER,

            chat_id INTEGER,

            chat_name TEXT,

            UNIQUE(user_id, chat_id)

        )

    """)

    

    # Таблица хранилища хешей кодов

    cursor.execute("""

        CREATE TABLE IF NOT EXISTS phone_code_hashes (

            user_id INTEGER PRIMARY KEY,

            phone_code_hash TEXT

        )

    """)

    

    # Таблица для хранения нескольких аккаунтов на пользователя

    cursor.execute("""

        CREATE TABLE IF NOT EXISTS user_accounts (

            id INTEGER PRIMARY KEY AUTOINCREMENT,

            bot_user_id INTEGER,

            account_number INTEGER,

            telegram_id INTEGER,

            username TEXT,

            first_name TEXT,

            phone TEXT,

            is_active BOOLEAN DEFAULT 0,

            UNIQUE(bot_user_id, account_number)

        )

    """)

    

    # Таблица VIP пользователей

    cursor.execute("""

        CREATE TABLE IF NOT EXISTS vip_users (

            user_id INTEGER PRIMARY KEY,

            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

        )

    """)

    

    conn.commit()

    conn.close()





def add_or_update_user(user_id: int, username: str, first_name: str):

    """Добавить или обновить пользователя"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    cursor.execute("""

        INSERT OR REPLACE INTO users (user_id, username, first_name)

        VALUES (?, ?, ?)

    """, (user_id, username, first_name))

    

    conn.commit()

    conn.close()





def set_user_logged_in(user_id: int, is_logged_in: bool):

    """Установить статус логирования пользователя"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    cursor.execute("""

        UPDATE users SET is_logged_in = ? WHERE user_id = ?

    """, (is_logged_in, user_id))

    

    conn.commit()

    conn.close()





def get_user_status(user_id: int) -> Optional[bool]:

    """Получить статус логирования пользователя"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    cursor.execute("SELECT is_logged_in FROM users WHERE user_id = ?", (user_id,))

    result = cursor.fetchone()

    conn.close()

    

    return result[0] if result else None





def get_all_users() -> List[Tuple]:

    """Получить всех пользователей"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    cursor.execute("SELECT user_id, username, first_name, is_logged_in FROM users")

    result = cursor.fetchall()

    conn.close()

    

    return result





def start_login_session(user_id: int, phone_number: str):

    """Начать сессию логина"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    # Очищаем старую сессию этого пользователя если была (для безопасности)

    cursor.execute("DELETE FROM login_sessions WHERE user_id = ?", (user_id,))

    

    cursor.execute("""

        INSERT OR REPLACE INTO login_sessions (user_id, phone_number, step)

        VALUES (?, ?, 'phone')

    """, (user_id, phone_number))

    

    conn.commit()

    conn.close()





def get_login_session(user_id: int) -> Optional[Tuple]:

    """Получить сессию логина"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    cursor.execute("SELECT phone_number, step FROM login_sessions WHERE user_id = ?", (user_id,))

    result = cursor.fetchone()

    conn.close()

    

    return result





def update_login_step(user_id: int, step: str):

    """Обновить шаг логина"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    cursor.execute("""

        UPDATE login_sessions SET step = ? WHERE user_id = ?

    """, (step, user_id))

    

    conn.commit()

    conn.close()





def save_phone_number(user_id: int, phone_number: str):

    """Сохранить номер телефона"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    cursor.execute("""

        INSERT OR REPLACE INTO login_sessions (user_id, phone_number)

        VALUES (?, ?)

    """, (user_id, phone_number))

    

    conn.commit()

    conn.close()





def delete_login_session(user_id: int):

    """Удалить сессию логина"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    cursor.execute("DELETE FROM login_sessions WHERE user_id = ?", (user_id,))

    

    conn.commit()

    conn.close()





def set_phone_code_hash(user_id: int, phone_code_hash: str):

    """Установить хеш кода подтверждения"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    cursor.execute("""

        INSERT OR REPLACE INTO phone_code_hashes (user_id, phone_code_hash)

        VALUES (?, ?)

    """, (user_id, phone_code_hash))

    

    conn.commit()

    conn.close()





def get_phone_code_hash(user_id: int) -> Optional[str]:

    """Получить хеш кода подтверждения"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    cursor.execute("SELECT phone_code_hash FROM phone_code_hashes WHERE user_id = ?", (user_id,))

    result = cursor.fetchone()

    conn.close()

    

    return result[0] if result else None





def add_tracked_chat(user_id: int, chat_id: int, chat_name: str) -> bool:

    """Добавить чат в отслеживание. Возвращает True если успешно, False если чат уже есть"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    try:

        cursor.execute("""

            INSERT INTO tracked_chats (user_id, chat_id, chat_name)

            VALUES (?, ?, ?)

        """, (user_id, chat_id, chat_name))

        conn.commit()

        conn.close()

        return True

    except sqlite3.IntegrityError:

        conn.close()

        return False





def remove_tracked_chat(user_id: int, chat_id: int):

    """Удалить чат из отслеживания"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    cursor.execute("""

        DELETE FROM tracked_chats WHERE user_id = ? AND chat_id = ?

    """, (user_id, chat_id))

    

    conn.commit()

    conn.close()





def get_tracked_chats(user_id: int) -> List[Tuple]:

    """Получить все отслеживаемые чаты пользователя"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    cursor.execute("""

        SELECT chat_id, chat_name FROM tracked_chats WHERE user_id = ?

    """, (user_id,))

    result = cursor.fetchall()

    conn.close()

    

    return result





def is_chat_tracked(user_id: int, chat_id: int) -> bool:

    """Проверить, отслеживается ли чат"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    cursor.execute("""

        SELECT 1 FROM tracked_chats WHERE user_id = ? AND chat_id = ?

    """, (user_id, chat_id))

    result = cursor.fetchone()

    conn.close()

    

    return result is not None





def add_broadcast_chat(user_id: int, chat_id: int, chat_name: str) -> bool:

    """Добавить чат в рассылку. Возвращает True если успешно, False если чат уже есть"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    try:

        cursor.execute("""

            INSERT INTO broadcast_chats (user_id, chat_id, chat_name)

            VALUES (?, ?, ?)

        """, (user_id, chat_id, chat_name))

        conn.commit()

        conn.close()

        return True

    except sqlite3.IntegrityError:

        conn.close()

        return False





def remove_broadcast_chat(user_id: int, chat_id: int):

    """Удалить чат из рассылки"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    cursor.execute("""

        DELETE FROM broadcast_chats WHERE user_id = ? AND chat_id = ?

    """, (user_id, chat_id))

    

    conn.commit()

    conn.close()





def get_broadcast_chats(user_id: int) -> List[Tuple]:

    """Получить все чаты рассылки пользователя с retry при database locked"""

    def _get():

        conn = sqlite3.connect(DB_PATH, timeout=30.0)

        cursor = conn.cursor()

        

        cursor.execute("""

            SELECT chat_id, chat_name FROM broadcast_chats WHERE user_id = ?

        """, (user_id,))

        result = cursor.fetchall()

        conn.close()

        return result

    

    return _retry_db_operation(_get, max_retries=3, base_delay=0.5)





def is_chat_in_broadcast(user_id: int, chat_id: int) -> bool:

    """Проверить, добавлен ли чат в рассылку"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    cursor.execute("""

        SELECT 1 FROM broadcast_chats WHERE user_id = ? AND chat_id = ?

    """, (user_id, chat_id))

    result = cursor.fetchone()

    conn.close()

    

    return result is not None





# Функции для работы с мультиаккаунтами



def add_user_account(bot_user_id: int, telegram_id: int, username: str, first_name: str, phone: str) -> int:

    """Добавить новый аккаунт для пользователя. Возвращает номер аккаунта"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    # Получаем следующий номер аккаунта

    cursor.execute("SELECT MAX(account_number) FROM user_accounts WHERE bot_user_id = ?", (bot_user_id,))

    result = cursor.fetchone()

    next_account = (result[0] or 0) + 1

    

    # Добавляем новый аккаунт

    cursor.execute("""

        INSERT INTO user_accounts (bot_user_id, account_number, telegram_id, username, first_name, phone, is_active)

        VALUES (?, ?, ?, ?, ?, ?, 1)

    """, (bot_user_id, next_account, telegram_id, username, first_name, phone))

    

    # Деактивируем все остальные аккаунты

    cursor.execute("""

        UPDATE user_accounts SET is_active = 0 WHERE bot_user_id = ? AND account_number != ?

    """, (bot_user_id, next_account))

    

    conn.commit()

    conn.close()

    

    return next_account





def add_user_account_with_number(bot_user_id: int, account_number: int, telegram_id: int, username: str, first_name: str, phone: str) -> bool:

    """Добавить аккаунт с конкретным номером (для восстановления из файлов)"""

    try:

        conn = sqlite3.connect(DB_PATH, timeout=30.0)

        cursor = conn.cursor()

        

        # Проверяем не существует ли уже такой аккаунт

        cursor.execute(

            "SELECT account_number FROM user_accounts WHERE bot_user_id = ? AND account_number = ?",

            (bot_user_id, account_number)

        )

        

        if cursor.fetchone():

            # Аккаунт уже существует, обновляем его

            cursor.execute("""

                UPDATE user_accounts 

                SET telegram_id = ?, username = ?, first_name = ?, phone = ?

                WHERE bot_user_id = ? AND account_number = ?

            """, (telegram_id, username, first_name, phone, bot_user_id, account_number))

        else:

            # Добавляем новый аккаунт

            cursor.execute("""

                INSERT INTO user_accounts (bot_user_id, account_number, telegram_id, username, first_name, phone, is_active)

                VALUES (?, ?, ?, ?, ?, ?, 1)

            """, (bot_user_id, account_number, telegram_id, username, first_name, phone))

        

        conn.commit()

        conn.close()

        return True

    except Exception as e:

        print(f"Ошибка при добавлении аккаунта: {str(e)}")

        return False





def get_user_accounts(bot_user_id: int) -> List[Tuple]:

    """Получить все аккаунты пользователя с retry при database locked"""

    def _get():

        conn = sqlite3.connect(DB_PATH, timeout=30.0)

        cursor = conn.cursor()

        

        cursor.execute("""

            SELECT account_number, telegram_id, username, first_name, is_active 

            FROM user_accounts WHERE bot_user_id = ?

            ORDER BY account_number

        """, (bot_user_id,))

        result = cursor.fetchall()

        conn.close()

        return result

    

    return _retry_db_operation(_get, max_retries=3, base_delay=0.5)





def get_active_account(bot_user_id: int) -> Optional[Tuple]:

    """Получить активный аккаунт пользователя"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    cursor.execute("""

        SELECT account_number, telegram_id, username, first_name, phone

        FROM user_accounts WHERE bot_user_id = ? AND is_active = 1

    """, (bot_user_id,))

    result = cursor.fetchone()

    conn.close()

    

    return result





def set_active_account(bot_user_id: int, account_number: int):

    """Установить активный аккаунт"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    # Деактивируем все аккаунты

    cursor.execute("UPDATE user_accounts SET is_active = 0 WHERE bot_user_id = ?", (bot_user_id,))

    

    # Активируем нужный

    cursor.execute("""

        UPDATE user_accounts SET is_active = 1 WHERE bot_user_id = ? AND account_number = ?

    """, (bot_user_id, account_number))

    

    conn.commit()

    conn.close()





def copy_account_data(bot_user_id: int, from_account: int, to_account: int):

    """Скопировать данные (чаты) от одного аккаунта к другому"""

    conn = sqlite3.connect(DB_PATH, timeout=30.0)

    cursor = conn.cursor()

    

    # Копируем отслеживаемые чаты

    cursor.execute("""

        SELECT user_id, chat_id, chat_name FROM tracked_chats 

        WHERE user_id = ? LIMIT 1

    """, (bot_user_id,))

    

    # Здесь нужна индивидуальная логика для отслеживания по аккаунтам

    # На данный момент просто копируем

    

    conn.close()





# Функции для сохранения конфига рассылки (простое JSON сохранение)

import json



def save_broadcast_config(user_id: int, config: dict):
    """?????????????????? ???????????? ???????????????? ?? ???????? ?? ?????????????????? ???????????? ?? ???????????????????? ????????????"""

    user_broadcast_dir(user_id)
    config_file = broadcast_config_path(user_id)
    backup_file = broadcast_config_backup_path(user_id)

    try:
        # ?????????????? ?????????????????? ?????????? ???????? ???????? ????????????????????
        if config_file.exists():
            try:
                shutil.copy2(config_file, backup_file)
            except Exception as e:
                print(f"??????  ???????????? ???????????????? backup ?????? {user_id}: {e}")

        # ?????????????????? ?????????? ???????????? ?? ???????????????????? ????????????????????
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)

    except Exception as e:
        print(f"??? ???????????? ???????????????????? ?????????????? ?????? {user_id}: {e}")
        # ???????? ???????????? ?????? ????????????????????, ???????????????? ???????????????????????? ???? backup
        if backup_file.exists():
            try:
                shutil.copy2(backup_file, config_file)
                print(f"???? ???????????????????????? backup ???????????? ?????? {user_id}")
            except Exception as restore_error:
                print(f"??? ???????????? ???????????????????????????? backup: {restore_error}")


def load_broadcast_config(user_id: int) -> dict:
    """?????????????????? ???????????? ???????????????? ???? ?????????? ?? fallback ???? backup"""

    user_broadcast_dir(user_id)
    config_file = broadcast_config_path(user_id)
    backup_file = broadcast_config_backup_path(user_id)

    # ???????? ?? ??????? ???????????? ? ?????
    legacy_config = Path(__file__).parent / f"broadcast_config_{user_id}.json"
    legacy_backup = Path(__file__).parent / f"broadcast_config_{user_id}.json.backup"
    if legacy_config.exists() and not config_file.exists():
        try:
            shutil.move(str(legacy_config), str(config_file))
        except Exception:
            pass
    if legacy_backup.exists() and not backup_file.exists():
        try:
            shutil.move(str(legacy_backup), str(backup_file))
        except Exception:
            pass

    if config_file.exists():
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"??????  ???????????? ???????????????? ?????????????????? ?????????????? {user_id}: {e}")
            # ???????????????? ?????????????????? backup
            if backup_file.exists():
                try:
                    with open(backup_file, 'r', encoding='utf-8') as f:
                        config = json.load(f)
                    print(f"???? ???????????????? backup ?????????????? ?????? {user_id}")
                    return config
                except Exception as backup_error:
                    print(f"??????  ???????????? ???????????????? backup: {backup_error}")

    # ?????????????? ?????????????????? ???????????? ???????? ???????????? ???? ????????????????????
    return {
        "text": "",
        "count": 1,
        "interval": 1,
        "parse_mode": "HTML"
    }


# === VIP ?????????????? ===



def add_vip_user(user_id: int) -> bool:

    """Добавить пользователя в VIP"""

    try:

        conn = sqlite3.connect(DB_PATH, timeout=30.0)

        cursor = conn.cursor()

        

        # Проверяем есть ли уже

        cursor.execute("SELECT user_id FROM vip_users WHERE user_id = ?", (user_id,))

        if cursor.fetchone():

            conn.close()

            return False

        

        cursor.execute("INSERT INTO vip_users (user_id) VALUES (?)", (user_id,))

        conn.commit()

        conn.close()

        return True

    except Exception as e:

        print(f"❌ Ошибка при добавлении VIP юзера: {str(e)}")

        return False





def remove_vip_user(user_id: int) -> bool:

    """Удалить пользователя из VIP"""

    try:

        conn = sqlite3.connect(DB_PATH, timeout=30.0)

        cursor = conn.cursor()

        

        cursor.execute("DELETE FROM vip_users WHERE user_id = ?", (user_id,))

        

        if cursor.rowcount == 0:

            conn.close()

            return False

        

        conn.commit()

        conn.close()

        return True

    except Exception as e:

        print(f"❌ Ошибка при удалении VIP юзера: {str(e)}")

        return False





def is_vip_user(user_id: int) -> bool:

    """Проверить является ли пользователь VIP"""

    try:

        conn = sqlite3.connect(DB_PATH, timeout=30.0)

        cursor = conn.cursor()

        

        cursor.execute("SELECT user_id FROM vip_users WHERE user_id = ?", (user_id,))

        result = cursor.fetchone()

        conn.close()

        

        return result is not None

    except Exception as e:

        print(f"❌ Ошибка при проверке VIP статуса: {str(e)}")

        return False





def get_all_vip_users() -> List[int]:

    """Получить список всех VIP юзеров"""

    try:

        conn = sqlite3.connect(DB_PATH, timeout=30.0)

        cursor = conn.cursor()

        

        cursor.execute("SELECT user_id FROM vip_users ORDER BY user_id")

        vip_list = [row[0] for row in cursor.fetchall()]

        

        conn.close()

        return vip_list

    except Exception as e:

        print(f"❌ Ошибка при получении VIP списка: {str(e)}")

        return []



