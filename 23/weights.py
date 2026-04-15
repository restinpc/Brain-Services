import re
import csv
import os


def parse_sql_values(value_str):
    """Парсит строку значений, учитывая кавычки SQL"""
    values = []
    current_val = []
    in_quotes = False

    for i, char in enumerate(value_str):
        if char == "'" and (i == 0 or value_str[i - 1] != '\\'):
            in_quotes = not in_quotes
        elif char == ',' and not in_quotes:
            val = "".join(current_val).strip()
            # Убираем кавычки вокруг значения
            if val.startswith("'") and val.endswith("'"):
                val = val[1:-1]
            values.append(val)
            current_val = []
        else:
            current_val.append(char)

    # Добавляем последнее значение
    if current_val:
        val = "".join(current_val).strip()
        if val.startswith("'") and val.endswith("'"):
            val = val[1:-1]
        values.append(val)
    return values


def parse_index_table(filename):
    print(f" Чтение файла: {filename}")

    if not os.path.exists(filename):
        print(f" Файл {filename} не найден")
        return []

    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()

    # --- ИСПРАВЛЕНИЕ: Используем | для флагов ---
    match = re.search(r"INSERT INTO `brain_calendar_event_index` VALUES\s+(.*?);", content, re.DOTALL | re.IGNORECASE)

    if not match:
        print(" Блок INSERT не найден")
        return []

    values_block = match.group(1)

    # Надежное разбиение строк SQL дампа по "),\n" или "),"
    # Это безопаснее, чем регулярка, если внутри текста есть скобки
    raw_rows = values_block.split("),\n")
    if len(raw_rows) == 1:
        raw_rows = values_block.split("),")

    events = []
    for row_str in raw_rows:
        # Чистим строку от начальных/конечных скобок
        clean_row = row_str.strip().strip("();,")
        if not clean_row: continue

        parts = parse_sql_values(clean_row)

        # Индексы: 0=EventId, 7=EventType
        if len(parts) >= 8:
            try:
                event_id = int(parts[0])
                event_type_sql = int(parts[7])

                # Логика: если в SQL тип 2 (праздники) -> это 1 (циклические)
                # Если тип 1 (новости) -> это 0 (рутинные)
                # Проверьте, совпадает ли это с вашей базой!
                event_type = '1' if event_type_sql == 2 else '0'

                events.append({'EventId': event_id, 'EventType': event_type})
            except ValueError:
                continue

    print(f" Найдено событий: {len(events)}")
    return events


def generate_binary_matrix(events):
    """Генерирует бинарную матрицу (полный перебор)"""
    all_codes = []

    for event in events:
        eid = event['EventId']
        etype = event['EventType']

        # 1. Базовые коды (без часов) - есть у ВСЕХ событий
        # В вашем примере: 1_0_0, 1_0_1, 1_1_0, 1_1_1
        # То есть: ID_TYPE_MODE
        all_codes.append(f"{eid}_{etype}_0")
        all_codes.append(f"{eid}_{etype}_1")

        # 2. Дополнительные коды с часами (только для циклических type=1)
        # В вашем примере: 2_1_0_1, 2_1_0_-1
        if etype == '1':
            for hour in range(-12, 13):
                # Просто число, например "-1", "5", "0"
                # Если нужен плюс ("+5"), используйте f"{hour:+d}"
                hour_str = str(hour)

                # mode=0 с часами
                all_codes.append(f"{eid}_{etype}_0_{hour_str}")
                # mode=1 с часами
                all_codes.append(f"{eid}_{etype}_1_{hour_str}")

    return all_codes


if __name__ == "__main__":
    input_file = "EcoCal_Processed.sql"
    output_file = "weights_table.csv"

    events = parse_index_table(input_file)

    if events:
        codes = generate_binary_matrix(events)

        # Сохраняем
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for code in codes:
                writer.writerow([code])

        print(f" Успешно сохранено {len(codes)} кодов в {output_file}")

        # Проверка (выводим первые 10, чтобы убедиться в формате)
        print("\n--- Примеры сгенерированных кодов ---")
        for c in codes[:10]:
            print(c)
