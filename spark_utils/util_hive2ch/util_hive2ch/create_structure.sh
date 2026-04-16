#!/bin/bash
# create_structure.sh

PROJECT_DIR="util_hive2ch"
mkdir -p "$PROJECT_DIR"/{dags,jobs,sql,toolbox,workflow,scripts,src,docs}

# Функция для извлечения и сохранения файла
extract_and_save() {
    local filename="$1"
    local content="$2"

    # Определяем целевую папку
    case "$filename" in
        dag_*|dag_example.py|build_dag.sh|generate_dag.py|dag_template.py.template)
            target="$PROJECT_DIR/dags/$filename"
            ;;
        job_*|build_job.sh|job_template.py.template)
            target="$PROJECT_DIR/jobs/$filename"
            ;;
        ddl_*)
            target="$PROJECT_DIR/sql/$filename"
            ;;
        build_generate_ddl*|generate_ddl.py|generate_ddl.sh|generate_ddl.sh.template)
            target="$PROJECT_DIR/toolbox/$filename"
            ;;
        workflow.xml)
            target="$PROJECT_DIR/workflow/$filename"
            ;;
        build_creds.sh|build_local_start.sh|build_oozie_start.sh|local_start.sh|oozie_start.sh|*.template)
            target="$PROJECT_DIR/scripts/$filename"
            ;;
        img*)
            target="$PROJECT_DIR/docs/$filename"
            ;;
        *)
            target="$PROJECT_DIR/$filename"
            ;;
    esac

    mkdir -p "$(dirname "$target")"
    printf "%s" "$content" > "$target"
    [[ "$filename" == *.sh ]] && chmod +x "$target" 2>/dev/null
    echo "Created: $target"
}

# Читаем и парсим входной файл
current_file=""
current_content=""

while IFS= read -r line || [ -n "$line" ]; do
    if [[ "$line" =~ ^===\ ([^=]+)\.txt\ ===$ ]]; then
        # Сохраняем предыдущий файл
        if [[ -n "$current_file" ]]; then
            extract_and_save "$current_file" "$current_content"
        fi
        # Начинаем новый файл
        current_file="${BASH_REMATCH[1]}"
        current_content=""
    else
        if [[ -n "$current_file" ]]; then
            current_content+="$line"$'\n'
        fi
    fi
done < "${1:-/dev/stdin}"

# Сохраняем последний файл
if [[ -n "$current_file" ]]; then
    extract_and_save "$current_file" "$current_content"
fi

# Создаем пустые __init__.py
touch "$PROJECT_DIR/src/__init__.py" "$PROJECT_DIR/jobs/__init__.py" 2>/dev/null

echo "✅ Проект создан в папке: $PROJECT_DIR/"