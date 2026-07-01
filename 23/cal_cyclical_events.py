import re
import pandas as pd
import hashlib
import csv
import sys

# Настройки файлов
INPUT_FILE = 'EcoCal.sql'
OUTPUT_FILE = 'EcoCal_Processed.sql'

# Список колонок в том порядке, как они идут в INSERT INTO brain_calendar
COLUMNS = [
    'Id', 'EventType', 'TimeMode', 'Processed', 'Url', 'EventName',
    'Importance', 'CurrencyCode', 'ForecastValue', 'PreviousValue',
    'OldPreviousValue', 'ActualValue', 'ReleaseDate', 'ImpactDirection',
    'ImpactValue', 'ImpactValueF', 'Country', 'CountryName', 'FullDate'
]

# Ключевые поля, определяющие уникальность циклического события
# (ActualValue здесь нет, как мы и договорились)
KEY_COLUMNS = [
    'Url', 'EventName', 'Country', 'CurrencyCode',
    'Importance', 'EventType', 'TimeMode'
]


def parse_sql_values(value_str):
    """
    Парсит строку значений SQL (1, 'text', NULL) в список Python.
    Учитывает кавычки и NULL.
    """
    values = []
    current_val = []
    in_quotes = False

    for char in value_str:
        if char == "'" and (not current_val or current_val[-1] != '\\'):
            in_quotes = not in_quotes
        elif char == ',' and not in_quotes:
            val = "".join(current_val).strip()
            values.append(val)
            current_val = []
            continue
        current_val.append(char)

    # Добавляем последнее значение
    if current_val:
        values.append("".join(current_val).strip())

    # Очистка значений
    cleaned_values = []
    for v in values:
        if v.upper() == 'NULL':
            cleaned_values.append(None)
        elif v.startswith("'") and v.endswith("'"):
            cleaned_values.append(v[1:-1])  # Убираем кавычки
        else:
            try:
                cleaned_values.append(float(v) if '.' in v else int(v))
            except ValueError:
                cleaned_values.append(v)
    return cleaned_values


def extract_data_from_sql(filename):
    print(f"Чтение {filename}...")
    data = []

    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()

    # Находим блок VALUES
    match = re.search(r"INSERT INTO `brain_calendar` .*? VALUES\s+(.*);", content, re.DOTALL)
    if not match:
        print("Не найден блок INSERT INTO")
        return pd.DataFrame()

    values_block = match.group(1)

    # Умный парсер, который не ломается о запятые и скобки внутри текста
    rows = []
    current_row_chars = []
    in_quotes = False
    depth = 0

    for i, char in enumerate(values_block):
        # Обработка кавычек (учитываем экранирование \')
        if char == "'" and (i == 0 or values_block[i - 1] != '\\'):
            in_quotes = not in_quotes

        # Отслеживаем вложенность скобок (начало и конец записи)
        if char == '(' and not in_quotes:
            depth += 1
        elif char == ')' and not in_quotes:
            depth -= 1

        # Если мы вне скобок и встречаем запятую - это разделитель строк
        if char == ',' and depth == 0 and not in_quotes:
            # Конец строки данных
            row_str = "".join(current_row_chars).strip()
            if row_str:
                # Убираем внешние скобки ( ... )
                if row_str.startswith('(') and row_str.endswith(')'):
                    row_str = row_str[1:-1]
                rows.append(row_str)
            current_row_chars = []
        else:
            current_row_chars.append(char)

    # Добавляем последнюю строку
    if current_row_chars:
        row_str = "".join(current_row_chars).strip()
        if row_str.startswith('(') and row_str.endswith(')'):
            rows.append(row_str[1:-1])

    # Парсим значения внутри каждой строки
    count = 0
    for row_str in rows:
        row_data = parse_sql_values(row_str)
        if len(row_data) == len(COLUMNS):
            data.append(row_data)
            count += 1
        else:
            # Это поможет увидеть реальную проблему, если она останется
            print(f"Warning: Row length {len(row_data)} != {len(COLUMNS)}. Row: {row_str[:50]}...")

    print(f"Загружено строк: {len(data)}")
    return pd.DataFrame(data, columns=COLUMNS)

def generate_sql_value(val):
    if val is None or pd.isna(val):
        return "NULL"
    if isinstance(val, (int, float)):
        return str(val)
    # Экранирование кавычек для SQL
    return "'" + str(val).replace("'", "''").replace("\\", "\\\\") + "'"


def main():
    # 1. Загрузка данных
    df = extract_data_from_sql(INPUT_FILE)

    # 2. Создание индекса событий
    print("Обработка циклических событий...")

    # Заменяем None на пустые строки для группировки (чтобы не терять данные),
    # но лучше оставить как есть и использовать dropna=False

    # Группируем по ключевым полям
    # size() считает кол-во появлений
    grouped = df.groupby(KEY_COLUMNS, dropna=False).size().reset_index(name='OccurrenceCount')

    # Оставляем только те, что повторяются > 1 раза (как вы просили)
    # ИЛИ убираем этот фильтр, если хотим индексировать ВСЕ события.
    # Обычно лучше индексировать всё, чтобы структура была единой.
    # Но следуя вашему ТЗ:
    recurring_events = grouped[grouped['OccurrenceCount'] > 1].copy()

    # Добавляем даты First/Last Occurrence
    agg_funcs = {'FullDate': ['min', 'max']}
    time_stats = df.groupby(KEY_COLUMNS, dropna=False).agg(agg_funcs).reset_index()
    time_stats.columns = KEY_COLUMNS + ['FirstOccurrence', 'LastOccurrence']

    # Объединяем статистику
    index_table = pd.merge(recurring_events, time_stats, on=KEY_COLUMNS)

    # Генерируем ID
    index_table['EventId'] = range(1, len(index_table) + 1)

    # Генерируем Hash (на всякий случай, как вы хотели)
    def get_hash(row):
        s = "".join([str(row[c]) for c in KEY_COLUMNS])
        return hashlib.md5(s.encode('utf-8')).hexdigest()

    index_table['EventHash'] = index_table.apply(get_hash, axis=1)

    print(f"Найдено уникальных циклических событий: {len(index_table)}")

    # 3. Объединение с основной таблицей
    print("Присвоение ID строкам...")
    merged_df = pd.merge(df, index_table, on=KEY_COLUMNS, how='left')

    # 4. Генерация выходного файла
    print(f"Генерация {OUTPUT_FILE}...")

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        # Заголовки
        f.write("-- Автоматически сгенерированный дамп с индексами\n")
        f.write("SET NAMES utf8mb4;\n\n")

        # 4.1 Создание таблицы индексов
        f.write("-- 1. Таблица индексов циклических событий\n")
        f.write("DROP TABLE IF EXISTS `brain_calendar_event_index`;\n")
        f.write("CREATE TABLE `brain_calendar_event_index` (\n")
        f.write("  `EventId` INT NOT NULL AUTO_INCREMENT,\n")
        f.write("  `EventHash` CHAR(32) NOT NULL,\n")
        f.write("  `Url` VARCHAR(400),\n")
        f.write("  `EventName` VARCHAR(400),\n")
        f.write("  `Country` MEDIUMINT,\n")
        f.write("  `CurrencyCode` VARCHAR(3),\n")
        f.write("  `Importance` VARCHAR(40),\n")
        f.write("  `EventType` INT,\n")
        f.write("  `TimeMode` INT,\n")
        f.write("  `FirstOccurrence` DATETIME,\n")
        f.write("  `LastOccurrence` DATETIME,\n")
        f.write("  `OccurrenceCount` INT,\n")
        f.write("  PRIMARY KEY (`EventId`),\n")
        f.write("  UNIQUE KEY `idx_hash` (`EventHash`)\n")
        f.write(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n\n")

        # 4.2 Вставка данных в таблицу индексов
        if not index_table.empty:
            f.write("INSERT INTO `brain_calendar_event_index` VALUES\n")
            lines = []
            cols_idx = ['EventId', 'EventHash'] + KEY_COLUMNS + ['FirstOccurrence', 'LastOccurrence', 'OccurrenceCount']

            for _, row in index_table.iterrows():
                vals = [generate_sql_value(row[c]) for c in cols_idx]
                lines.append(f"({','.join(vals)})")

            f.write(",\n".join(lines) + ";\n\n")

        # 4.3 Создание основной таблицы (сразу с EventId)
        f.write("-- 2. Основная таблица календаря\n")
        f.write("DROP TABLE IF EXISTS `brain_calendar`;\n")
        f.write("CREATE TABLE `brain_calendar` (\n")
        f.write("  `Id` int NOT NULL AUTO_INCREMENT,\n")
        # Новая колонка EventId идет второй
        f.write("  `EventId` int DEFAULT NULL COMMENT 'Ссылка на brain_calendar_event_index',\n")
        f.write("  `EventType` int NOT NULL DEFAULT '0',\n")
        f.write("  `TimeMode` int NOT NULL DEFAULT '0',\n")
        f.write("  `Processed` int NOT NULL DEFAULT '0',\n")
        f.write("  `Url` varchar(400) DEFAULT NULL,\n")
        f.write("  `EventName` varchar(400) DEFAULT NULL,\n")
        f.write("  `Importance` varchar(40) DEFAULT NULL,\n")
        f.write("  `CurrencyCode` varchar(3) DEFAULT NULL,\n")
        f.write("  `ForecastValue` float DEFAULT NULL,\n")
        f.write("  `PreviousValue` float DEFAULT NULL,\n")
        f.write("  `OldPreviousValue` float DEFAULT NULL,\n")
        f.write("  `ActualValue` float DEFAULT NULL,\n")
        f.write("  `ReleaseDate` bigint DEFAULT NULL,\n")
        f.write("  `ImpactDirection` tinyint DEFAULT NULL,\n")
        f.write("  `ImpactValue` float DEFAULT NULL,\n")
        f.write("  `ImpactValueF` float DEFAULT NULL,\n")
        f.write("  `Country` mediumint DEFAULT NULL,\n")
        f.write("  `CountryName` varchar(40) DEFAULT NULL,\n")
        f.write("  `FullDate` datetime DEFAULT NULL,\n")
        f.write("  PRIMARY KEY (`Id`),\n")
        f.write("  KEY `idx_EventId` (`EventId`),\n")
        f.write("  KEY `Url_Country_FullDate` (`Url`,`Country`,`FullDate`)\n")
        f.write(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n\n")

        # 4.4 Вставка основных данных
        print("Запись основной таблицы...")
        f.write("INSERT INTO `brain_calendar` VALUES\n")

        # Подготовка колонок для вывода (EventId вставлен на 2-ю позицию)
        # Original: Id, EventType...
        # New: Id, EventId, EventType...

        batch_size = 5000
        total_rows = len(merged_df)

        for i in range(0, total_rows, batch_size):
            batch = merged_df.iloc[i:i + batch_size]
            lines = []
            for _, row in batch.iterrows():
                # Собираем значения в нужном порядке
                # 1. Id
                vals = [generate_sql_value(row['Id'])]

                # 2. EventId (из merged_df, может быть NULL/NaN)
                eid = row['EventId']
                if pd.isna(eid):
                    vals.append("NULL")
                else:
                    vals.append(str(int(eid)))

                # 3. Остальные колонки (начиная с EventType)
                for c in COLUMNS[1:]:
                    vals.append(generate_sql_value(row[c]))

                lines.append(f"({','.join(vals)})")

            end_char = ",\n" if i + batch_size < total_rows else ";\n"
            f.write(",\n".join(lines) + end_char)

    print("Готово! Файл EcoCal_Processed.sql создан.")


if __name__ == "__main__":
    main()
