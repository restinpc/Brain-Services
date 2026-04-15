import os
import re

def remove_emojis(text):
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"
        "\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F9FF"
        "\U0001F1E0-\U0001F1FF"
        "\U00002600-\U000027BF"
        "\U0001FA00-\U0001FA9F"
        "\U0001FAB0-\U0001FABF"
        "\U0001FAC0-\U0001FAFF"
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]+",
        flags=re.UNICODE
    )
    return emoji_pattern.sub("", text)

def process_directory(root_dir="."):
    changed = 0
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if not filename.endswith(".py"):
                continue
            filepath = os.path.join(dirpath, filename)
            with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
                original = f.read()
            cleaned = remove_emojis(original)
            if cleaned != original:
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(cleaned)
                print(f"  Cleaned: {filepath}")
                changed += 1
    print(f"\nDone. Files modified: {changed}")

if __name__ == "__main__":
    process_directory(".")