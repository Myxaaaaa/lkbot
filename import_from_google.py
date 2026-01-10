"""
Скрипт для импорта данных из Google Таблиц в бота
Поддерживает импорт из Google Sheets API или из текстового файла
"""

import json
import re
import csv
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

# Путь к файлу данных бота
DATA_FILE = Path(__file__).parent / "lk_registry.json"

# Маппинг статусов из таблицы в статусы бота
STATUS_MAPPING = {
    "у дропа": "в работе",
    "отдых": "на отдыхе",
    "блок": "заблокирован",
    "заблокирован": "заблокирован",
    "вылет": "Вылет",
    "потерялся": "заблокирован",
    "актив": "в работе",
    "виртуалка": "в работе",
    "ждем карту": "NEW-white",
    "ждем ответа": "NEW-white",
    "статус": "NEW-white",
    "сменил номер": "в работе",
    "актив\\бинанс": "в работе",
    "актив/бинанс": "в работе",
    "Need to white": "NEW-white",
    "NEW-white": "NEW-white",
    "NEW-not white": "NEW-not white",
    "в работе": "в работе",
    "на отдыхе": "на отдыхе",
    "Вылет": "Вылет",
}

# Статусы, требующие указания остатка средств
STATUS_REQUIRING_FUNDS = {"Вылет", "заблокирован"}


def normalize_phone(phone: str) -> str:
    """Нормализует номер телефона, добавляя +996 если нужно"""
    if not phone or phone.strip() == "":
        return ""
    
    phone_clean = re.sub(r'[^\d+]', '', phone.strip())
    
    # Если номер пустой после очистки
    if not phone_clean:
        return ""
    
    # Если номер начинается с +996, оставляем как есть
    if phone_clean.startswith('+996'):
        return phone_clean
    # Если номер начинается с 996, добавляем +
    elif phone_clean.startswith('996'):
        return '+' + phone_clean
    # Если номер короткий (9 цифр), добавляем +996
    elif len(phone_clean) == 9 and phone_clean.isdigit():
        return '+996' + phone_clean
    # Если номер средний (10-11 цифр без кода страны), добавляем +996
    elif len(phone_clean) >= 10 and phone_clean.isdigit() and not phone_clean.startswith('996'):
        return '+996' + phone_clean[-9:]  # Берем последние 9 цифр
    # Если номер уже с плюсом, но не 996
    elif phone_clean.startswith('+') and len(phone_clean) > 4:
        return phone_clean
    # Иначе возвращаем как есть (может быть неправильный формат)
    return phone_clean


def normalize_card(card: str) -> str:
    """Нормализует номер карты, убирая лишние пробелы"""
    if not card or card.strip() == "":
        return ""
    # Убираем лишние пробелы и форматируем
    card = re.sub(r'\s+', ' ', card.strip())
    return card


def detect_bank(name: str, card: str = "") -> str:
    """Определяет банк по имени или карте"""
    name_lower = name.lower()
    card_clean = card.replace(' ', '')
    
    # Проверяем по имени
    if 'мбанк' in name_lower or 'манас' in name_lower:
        return "Мбанк"
    if 'бинанс' in name_lower:
        return "Бинанс"
    
    # Проверяем по карте (4177 обычно Мбанк, 4714 - другой банк)
    if card_clean.startswith('4177') or card_clean.startswith('9450') or card_clean.startswith('9356'):
        return "Мбанк"
    if card_clean.startswith('4714'):
        return "Другой банк"
    
    # По умолчанию
    return "Мбанк"


def map_status(status: str) -> str:
    """Маппит статус из таблицы в статус бота"""
    status_lower = status.lower().strip()
    return STATUS_MAPPING.get(status_lower, "в работе")


def parse_text_data(text_data: str) -> List[Dict[str, str]]:
    """Парсит текстовые данные в формат записей"""
    records = []
    lines = text_data.strip().split('\n')
    
    for line_num, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue
        
        # Убираем кавычки и лишние символы
        line = line.replace('"', '').strip()
        
        # Разбиваем строку по табуляции или множественным пробелам
        # Сначала пробуем табуляцию
        if '\t' in line:
            parts = line.split('\t')
        else:
            # Если нет табуляции, разбиваем по множественным пробелам
            parts = re.split(r'\s{3,}', line)
            if len(parts) < 4:
                # Пробуем разбить по двум пробелам
                parts = re.split(r'\s{2,}', line)
        
        parts = [p.strip() for p in parts if p.strip()]
        
        if len(parts) < 1:
            continue
        
        # Парсим данные (может быть 2-4 колонки)
        # Структура: A=ФИО, B=Телефон, C=Карта (или статус), D=Статус
        full_name = parts[0].strip()
        phone = parts[1].strip() if len(parts) > 1 else ""
        card = ""
        status = "в работе"
        
        # Определяем карту и статус
        if len(parts) >= 4:
            # Есть все 4 колонки: ФИО, Телефон, Карта, Статус
            card = parts[2].strip()
            status = parts[3].strip()
        elif len(parts) == 3:
            # Только 3 колонки: нужно определить, что в третьей - карта или статус
            third_col = parts[2].strip().lower()
            # Проверяем, является ли третья колонка статусом
            if (third_col in STATUS_MAPPING or 
                any(s in third_col for s in ['отдых', 'блок', 'вылет', 'актив', 'дропа', 'ждем', 'new'])):
                status = parts[2].strip()
                card = ""
            else:
                # Скорее всего это карта, статус по умолчанию
                card = parts[2].strip()
                status = "в работе"
        elif len(parts) == 2:
            # Только ФИО и телефон
            phone = parts[1].strip()
            card = ""
            status = "в работе"
        
        # Нормализуем данные
        phone = normalize_phone(phone)
        card = normalize_card(card)
        status = map_status(status)
        bank = detect_bank(full_name, card)
        
        # Пропускаем записи без имени
        if not full_name:
            try:
                print(f"⚠ Строка {line_num}: пропущена (нет имени)")
            except UnicodeEncodeError:
                print(f"⚠ Строка {line_num}: пропущена (нет имени)")
            continue
        
        # Создаем запись
        record = {
            "bank": bank,
            "full_name": full_name,
            "phone": phone,
            "card": card,
            "status": status,
        }
        
        # Добавляем остаток средств для определенных статусов
        if status in STATUS_REQUIRING_FUNDS:
            record["remaining_funds"] = "0"
        
        records.append(record)
    
    return records


def load_existing_records() -> List[Dict[str, str]]:
    """Загружает существующие записи из JSON"""
    if not DATA_FILE.exists():
        return []
    try:
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return []


def generate_id(records: List[Dict[str, str]]) -> str:
    """Генерирует новый ID"""
    if not records:
        return "1"
    max_id = max(int(rec.get("id", "0")) for rec in records if rec.get("id", "0").isdigit())
    return str(max_id + 1)


def clear_all_data() -> bool:
    """
    Очищает все данные в боте
    
    Returns:
        True если успешно очищено, False в случае ошибки
    """
    try:
        # Сохраняем пустой массив
        DATA_FILE.write_text("[]", encoding='utf-8')
        print("✅ Все данные в боте очищены")
        return True
    except Exception as e:
        print(f"❌ Ошибка при очистке данных: {e}")
        return False


def import_from_text(text_data: str, merge: bool = True) -> int:
    """
    Импортирует данные из текста
    
    Args:
        text_data: Текстовые данные для импорта
        merge: Если True, объединяет с существующими записями (по телефону)
    
    Returns:
        Количество импортированных записей
    """
    new_records = parse_text_data(text_data)
    existing_records = load_existing_records() if merge else []
    
    # Создаем словарь существующих записей по телефону для проверки дубликатов
    existing_phones = {rec.get("phone", ""): rec for rec in existing_records}
    
    imported_count = 0
    # Если merge=False, начинаем с ID=1, иначе продолжаем с существующих
    if merge:
        next_id = generate_id(existing_records)
    else:
        next_id = "1"
    
    for record in new_records:
        phone = record.get("phone", "")
        
        # Проверяем на дубликаты (только если merge=True)
        if merge and phone and phone in existing_phones:
            try:
                print(f"⚠ Пропущен дубликат: {record['full_name']} ({phone})")
            except UnicodeEncodeError:
                print(f"⚠ Пропущен дубликат: {record['full_name'].encode('ascii', 'ignore').decode()} ({phone})")
            continue
        
        # Добавляем ID
        record["id"] = next_id
        next_id = str(int(next_id) + 1)
        
        existing_records.append(record)
        imported_count += 1
        
        # Выводим только каждую 10-ю запись, чтобы не засорять вывод
        if imported_count % 10 == 0 or imported_count <= 5:
            try:
                print(f"✅ Импортирован: {record['full_name']} (ID: {record['id']})")
            except UnicodeEncodeError:
                print(f"✅ Импортирован: {record['full_name'].encode('ascii', 'ignore').decode()} (ID: {record['id']})")
    
    # Сохраняем
    DATA_FILE.write_text(
        json.dumps(existing_records, ensure_ascii=False, indent=2),
        encoding='utf-8'
    )
    
    return imported_count


def import_from_google_sheets_csv(sheet_id: str, gid: str = "0") -> int:
    """
    Импортирует данные из Google Sheets через CSV экспорт (публичный доступ)
    
    Args:
        sheet_id: ID Google таблицы (из URL)
        gid: ID листа (из URL, по умолчанию "0")
    
    Returns:
        Количество импортированных записей
    """
    try:
        # URL для экспорта CSV
        csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
        
        print(f"📥 Загрузка данных из Google Таблицы...")
        print(f"🔗 URL: {csv_url}")
        
        # Загружаем CSV с обработкой ошибок
        req = urllib.request.Request(csv_url)
        req.add_header('User-Agent', 'Mozilla/5.0')
        
        with urllib.request.urlopen(req, timeout=30) as response:
            csv_data = response.read().decode('utf-8')
        
        # Парсим CSV
        csv_reader = csv.reader(csv_data.splitlines())
        rows = list(csv_reader)
        
        if not rows:
            print("❌ Таблица пуста")
            return 0
        
        print(f"📊 Загружено строк из таблицы: {len(rows)}")
        
        # Пропускаем заголовок (первую строку) и конвертируем в текстовый формат
        text_lines = []
        for row_num, row in enumerate(rows[1:], start=2):  # Пропускаем заголовок, начинаем с 2
            # Очищаем пустые строки
            if not any(cell.strip() for cell in row):
                continue
            
            # Берем первые 4 колонки (A, B, C, D) - ФИО, Телефон, Карта, Статус
            row_data = row[:4] if len(row) >= 4 else row + [''] * (4 - len(row))
            # Объединяем через табуляцию
            text_lines.append('\t'.join(row_data))
        
        text_data = '\n'.join(text_lines)
        print(f"📝 Обработано строк данных: {len(text_lines)}")
        
        # Импортируем (merge=False, чтобы заменить все данные)
        return import_from_text(text_data, merge=False)
        
    except urllib.error.HTTPError as e:
        print(f"❌ HTTP ошибка при загрузке данных: {e.code} - {e.reason}")
        print("💡 Убедитесь, что таблица доступна публично")
        print("💡 Или используйте метод с credentials (import_from_google_sheets)")
        return 0
    except urllib.error.URLError as e:
        print(f"❌ Ошибка при загрузке данных из Google Таблицы: {e}")
        print("💡 Проверьте подключение к интернету")
        return 0
    except Exception as e:
        print(f"❌ Ошибка при импорте: {e}")
        import traceback
        traceback.print_exc()
        return 0


def import_from_google_sheets(sheet_id: str, range_name: str = "A:D", credentials_file: Optional[str] = None) -> int:
    """
    Импортирует данные из Google Sheets
    
    Args:
        sheet_id: ID Google таблицы (из URL)
        range_name: Диапазон ячеек (по умолчанию A:D)
        credentials_file: Путь к файлу credentials.json для Google API
    
    Returns:
        Количество импортированных записей
    """
    try:
        from google.oauth2.credentials import Credentials
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError
    except ImportError:
        print("❌ Ошибка: Не установлена библиотека google-api-python-client")
        print("Установите: pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib")
        return 0
    
    try:
        # Загружаем credentials
        if credentials_file and Path(credentials_file).exists():
            creds = service_account.Credentials.from_service_account_file(
                credentials_file,
                scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
            )
        else:
            print("❌ Не найден файл credentials.json")
            print("Создайте проект в Google Cloud Console и скачайте credentials.json")
            return 0
        
        # Создаем сервис
        service = build('sheets', 'v4', credentials=creds)
        
        # Читаем данные
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        
        values = result.get('values', [])
        
        if not values:
            print("❌ Таблица пуста")
            return 0
        
        # Пропускаем заголовок (первую строку)
        text_data = "\n".join(["\t".join(row) for row in values[1:]])
        
        return import_from_text(text_data, merge=True)
        
    except HttpError as error:
        print(f"❌ Ошибка Google API: {error}")
        return 0


if __name__ == "__main__":
    import sys
    import io
    
    # Настройка кодировки для Windows
    if sys.platform == 'win32':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    
    # ID Google Таблицы из URL
    GOOGLE_SHEET_ID = "1frJ4DEvdmLSuIzdqXhewjQRXXsW4xavwnCUoS7WzBQM"
    GOOGLE_SHEET_GID = "0"
    
    print("🔄 Очистка и импорт данных из Google Таблицы")
    print("=" * 60)
    
    # Шаг 1: Очищаем все данные в боте
    print("\n1️⃣ Очистка данных бота...")
    if not clear_all_data():
        print("❌ Не удалось очистить данные. Прерывание.")
        sys.exit(1)
    
    # Шаг 2: Импортируем данные из Google Таблицы
    print("\n2️⃣ Импорт данных из Google Таблицы...")
    imported = import_from_google_sheets_csv(GOOGLE_SHEET_ID, GOOGLE_SHEET_GID)
    
    print("\n" + "=" * 60)
    if imported > 0:
        print(f"✅ Успешно импортировано записей: {imported}")
        print(f"📁 Данные сохранены в: {DATA_FILE}")
    else:
        print("❌ Не удалось импортировать данные")
        print("💡 Проверьте доступность таблицы или используйте другой метод импорта")

