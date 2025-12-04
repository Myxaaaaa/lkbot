"""
Скрипт для импорта данных из Google Таблиц в бота
Поддерживает импорт из Google Sheets API или из текстового файла
"""

import json
import re
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
        full_name = parts[0].strip()
        phone = parts[1].strip() if len(parts) > 1 else ""
        card = parts[2].strip() if len(parts) > 2 else ""
        status = parts[3].strip() if len(parts) > 3 else "в работе"
        
        # Если статус пустой, но есть только 3 колонки, возможно статус в 3-й колонке
        if not status and len(parts) == 3:
            # Проверяем, является ли последняя колонка статусом
            last_part = parts[2].lower()
            if last_part in STATUS_MAPPING or any(s in last_part for s in ['отдых', 'блок', 'вылет', 'актив', 'дропа']):
                status = parts[2]
                card = ""
        
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
    next_id = generate_id(existing_records)
    
    for record in new_records:
        phone = record.get("phone", "")
        
        # Проверяем на дубликаты
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
    
    # Пример использования с текстовыми данными
    sample_data = """Болотов Алишер Болотович	755 202 976	4177 4901 5776 8559	у дропа
Мамбеталиев Темирлан Толомушевич	755 117 823	4177 4901 2860 9775	у дропа
Токтосунов Дастан Нурбекович	755 039 098	4177 4901 8629 0716	отдых"""
    
    print("📥 Импорт данных...")
    print("=" * 50)
    
    # Читаем данные из файла или используем встроенные
    data_file = Path(__file__).parent / "import_data.txt"
    
    if data_file.exists():
        print(f"📄 Чтение данных из {data_file}")
        text_data = data_file.read_text(encoding='utf-8')
    else:
        print("📝 Использование встроенных тестовых данных")
        print("💡 Создайте файл import_data.txt с данными для импорта")
        text_data = sample_data
    
    imported = import_from_text(text_data, merge=True)
    
    print("=" * 50)
    print(f"✅ Импортировано записей: {imported}")
    print(f"📁 Данные сохранены в: {DATA_FILE}")

