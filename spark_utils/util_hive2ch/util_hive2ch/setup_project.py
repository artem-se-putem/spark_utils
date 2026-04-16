#!/usr/bin/env python3
"""
Скрипт для парсинга файлов в формате === filename.txt === и создания структуры проекта
Запуск: python3 setup_project.py < all_files.txt
"""

import os
import re
import sys
from pathlib import Path

# Маппинг файлов в папки
FILE_TO_FOLDER = {
    # build скрипты
    "build_creds.sh": "scripts",
    "build_dag.sh": "dags",
    "build_generate_ddl.sh": "toolbox",
    "build_job.sh": "jobs",
    "build_local_start.sh": "scripts",
    "build_oozie_start.sh": "scripts",

    # dag файлы
    "dag_example.py": "dags",
    "dag_template.py.template": "dags",
    "generate_dag.py": "dags",

    # job файлы
    "job_template.py.template": "jobs",

    # ddl файлы
    "ddl_example.sql": "sql",
    "ddl_template.sql.template": "sql",

    # generate_ddl файлы
    "generate_ddl.py": "toolbox",
    "generate_ddl.sh": "toolbox",
    "generate_ddl.sh.template": "toolbox",

    # start скрипты
    "local_start.sh": "scripts",
    "local_start.sh.template": "scripts",
    "oozie_start.sh": "scripts",
    "oozie_start.sh.template": "scripts",

    # workflow
    "workflow.xml": "workflow",

    # изображения
    "img.png": "docs",
    "img2.png": "docs",
    "img3.png": "docs",
    "img4.png": "docs",
    "img7.png": "docs",
    "img8.png": "docs",
    "img9.png": "docs",
    "img12.png": "docs",
    "img16.png": "docs",
    "img17.png": "docs",
    "img18.png": "docs",
    "img19.png": "docs",
    "img20.png": "docs",
    "img21.png": "docs",
    "img22.png": "docs",
    "img24.png": "docs",
    "img25.png": "docs",
    "img26.png": "docs",
    "img27.png": "docs",
    "img28.png": "docs",
    "img29.png": "docs",
    "img30.png": "docs",
    "img33.png": "docs",
    "img35.png": "docs",
}

# Файлы, которые идут в корень
ROOT_FILES = [
    "common.env",
    "common.env.template",
    "common.sh",
    "tech.env",
    "tech.env.template",
    "log4j.properties",
    "main.py",
    "perform.py",
    "pull_project.py",
    "README.md",
    "creds.py",
    "creds_template.py.template",
]


def get_target_folder(filename):
    """Определяет папку для файла"""
    # Проверяем специальные маппинги
    if filename in FILE_TO_FOLDER:
        return FILE_TO_FOLDER[filename]

    # Проверяем по расширениям
    if filename.startswith("img") and filename.endswith(".png"):
        return "docs"
    if filename.endswith(".sh") or filename.endswith(".template"):
        if "dag" in filename:
            return "dags"
        if "job" in filename:
            return "jobs"
        if "ddl" in filename:
            return "sql"

    # По умолчанию в корень
    return "."


def parse_files_from_stdin():
    """Читает из stdin и парсит блоки === filename.txt ==="""
    # Читаем как бинарные данные, затем декодируем с заменой ошибок
    raw_content = sys.stdin.buffer.read()
    content = raw_content.decode('utf-8', errors='replace')

    # Регулярное выражение для поиска блоков === filename.txt ===
    pattern = r'=== (.+?)\.txt ===(.*?)==='

    files = {}
    matches = re.findall(pattern, content, re.DOTALL)

    for filename, file_content in matches:
        # Убираем лишние пробелы в начале/конце
        file_content = file_content.rstrip('\n')
        files[filename.strip()] = file_content

    return files


def create_project_structure(files):
    """Создает структуру папок и файлов"""

    # Создаем корневую папку проекта
    project_root = Path("util_hive2ch")
    if project_root.exists():
        print(f"Папка {project_root} уже существует. Удаляем...")
        import shutil
        shutil.rmtree(project_root)

    project_root.mkdir(parents=True)
    print(f"Создана корневая папка: {project_root}")

    # Создаем все необходимые подпапки
    folders = ["dags", "jobs", "sql", "toolbox", "workflow", "scripts", "docs", "src"]
    for folder in folders:
        (project_root / folder).mkdir(exist_ok=True)
        print(f"Создана папка: {folder}/")

    # Обрабатываем каждый файл
    for filename, content in files.items():
        # Определяем целевую папку
        target_folder = get_target_folder(filename)

        if target_folder == ".":
            target_path = project_root / filename
        else:
            target_path = project_root / target_folder / filename

        # Записываем файл
        try:
            with open(target_path, 'w', encoding='utf-8', errors='ignore') as f:
                f.write(content)

            # Делаем shell скрипты исполняемыми (на Unix)
            if filename.endswith('.sh') and sys.platform != 'win32':
                os.chmod(target_path, 0o755)

            print(f"Создан файл: {target_path.relative_to(project_root)}")
        except Exception as e:
            print(f"Ошибка при записи файла {filename}: {e}")

    # Создаем пустые __init__.py файлы для Python модулей
    for init_dir in ["src", "jobs"]:
        init_file = project_root / init_dir / "__init__.py"
        if not init_file.exists():
            init_file.touch()
            print(f"Создан файл: {init_dir}/__init__.py")

    # Создаем Makefile
    makefile_content = '''# Makefile для util_hive2ch

.PHONY: help keytab_init project_local-build local_start project_oozie-build project_load_oozie project_airflow-build project_load_airflow

help:
	@echo "Available commands:"
	@echo "  make keytab_init          - Initialize Kerberos keytab"
	@echo "  make project_local-build  - Build for local execution"
	@echo "  make local_start          - Run locally"
	@echo "  make project_oozie-build  - Build for Oozie"
	@echo "  make project_load_oozie   - Load to HDFS for Oozie"
	@echo "  make project_airflow-build - Build for Airflow"
	@echo "  make project_load_airflow  - Load to HDFS for Airflow"

keytab_init:
\tsource common.sh && keytab_init

project_local-build:
\tsource common.sh && LocalStartBuild
\tsource common.sh && CredsBuild
\tsource common.sh && GenerateDDLBuild
\tsource common.sh && JobBuild

local_start:
\tbash local_start.sh

project_oozie-build:
\tsource common.sh && OozieStartBuild
\tsource common.sh && CredsBuild

project_load_oozie:
\tsource common.sh && ProjectLoadOozie

project_airflow-build:
\tsource common.sh && DagBuild
\tsource common.sh && CredsBuild

project_load_airflow:
\tsource common.sh && ProjectLoadAirflow
'''

    makefile_path = project_root / "Makefile"
    with open(makefile_path, 'w', encoding='utf-8') as f:
        f.write(makefile_content)
    print(f"Создан файл: Makefile")

    print("\n" + "="*60)
    print("✅ Проект успешно создан в папке: util_hive2ch/")
    print("="*60)

    # Выводим структуру проекта
    print("\n📁 Структура проекта:")
    for root, dirs, files_in_dir in sorted(os.walk(project_root)):
        level = root.replace(str(project_root), '').count(os.sep)
        indent = '  ' * level
        folder_name = os.path.basename(root)
        if folder_name == str(project_root):
            folder_name = "util_hive2ch/"
        elif level > 0:
            print(f'{indent}├── {folder_name}/')
        else:
            print(f'{indent}{folder_name}')

        subindent = '  ' * (level + 1)
        for i, file in enumerate(sorted(files_in_dir)):
            is_last = (i == len(files_in_dir) - 1)
            prefix = '└──' if is_last else '├──'
            print(f'{subindent}{prefix} {file}')

    print("\n📝 Для использования перейдите в папку проекта:")
    print("  cd util_hive2ch")
    print("\n🔑 Сделайте keytab (на Linux):")
    print("  source common.sh && keytab_init")
    print("\n🚀 Для локального запуска:")
    print("  make project_local-build && make local_start")


def main():
    # Проверяем, есть ли данные в stdin
    if sys.stdin.isatty():
        print("❌ Использование: cat all_files.txt | python3 setup_project.py")
        print("   Или: python3 setup_project.py < all_files.txt")
        sys.exit(1)

    files = parse_files_from_stdin()

    if not files:
        print("❌ Не найдено ни одного файла в формате === filename.txt ===")
        sys.exit(1)

    print(f"📄 Найдено файлов: {len(files)}")
    create_project_structure(files)


if __name__ == "__main__":
    main()