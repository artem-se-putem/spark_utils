@echo off
chcp 65001 > nul
title Установка PySpark проекта
color 0A

echo ========================================
echo    УСТАНОВКА PYSPARK ПРОЕКТА
echo ========================================
echo.
echo Проект: D:\deals\pyspark_utils
echo Дата: %date%
echo.
echo Нажмите Enter для начала установки...
pause > nul

:: ========================================
:: 1. Проверка Python
:: ========================================
echo.
echo [1/7] Проверка Python 3.10...
if exist C:\Python310\python.exe (
    echo    ✅ Python 3.10 найден
    set PYTHON_PATH=C:\Python310\python.exe
) else (
    echo    ❌ Python 3.10 не найден
    echo    Установка Python 3.10...
    winget install --id Python.Python.3.10 -e --source winget
    if %errorlevel% equ 0 (
        echo    ✅ Python 3.10 установлен
    ) else (
        echo    ❌ Ошибка установки Python
        echo    Скачайте вручную: https://www.python.org/downloads/windows/
        pause
        exit /b 1
    )
)

:: ========================================
:: 2. Проверка pip
:: ========================================
echo.
echo [2/7] Обновление pip...
C:\Python310\python.exe -m pip install --upgrade pip
echo    ✅ pip обновлен

:: ========================================
:: 3. Установка необходимых пакетов
:: ========================================
echo.
echo [3/7] Установка Python пакетов...

:: Основные пакеты
C:\Python310\python.exe -m pip install pyspark==3.3.4
echo    ✅ PySpark 3.3.4 установлен

C:\Python310\python.exe -m pip install pandas
echo    ✅ Pandas установлен

C:\Python310\python.exe -m pip install numpy
echo    ✅ NumPy установлен

:: Jupyter и инструменты
C:\Python310\python.exe -m pip install jupyter notebook
echo    ✅ Jupyter установлен

C:\Python310\python.exe -m pip install jupyterlab
echo    ✅ Jupyter Lab установлен

C:\Python310\python.exe -m pip install ipykernel
echo    ✅ IPython kernel установлен

:: Инструменты для автодополнения
C:\Python310\python.exe -m pip install jedi
echo    ✅ Jedi установлен

C:\Python310\python.exe -m pip install pyarrow
echo    ✅ PyArrow установлен

C:\Python310\python.exe -m pip install findspark
echo    ✅ FindSpark установлен

:: Создание ядра для Jupyter
echo.
echo [4/7] Создание Jupyter ядра...
C:\Python310\python.exe -m ipykernel install --user --name pyspark310 --display-name "Python (PySpark 3.10)"
echo    ✅ Ядро pyspark310 создано

:: ========================================
:: 4. Проверка Hadoop
:: ========================================
echo.
echo [5/7] Проверка Hadoop...
if exist D:\deals\hadoop\bin\winutils.exe (
    echo    ✅ Hadoop файлы найдены
) else (
    echo    ⚠️ Hadoop файлы не найдены
    echo    Создание папки Hadoop...
    mkdir D:\deals\hadoop\bin 2>nul
    
    echo    Скачивание winutils.exe...
    powershell -Command "Invoke-WebRequest -Uri 'https://raw.githubusercontent.com/steveloughran/winutils/master/hadoop-3.3.4/bin/winutils.exe' -OutFile 'D:\deals\hadoop\bin\winutils.exe'"
    
    echo    Скачивание hadoop.dll...
    powershell -Command "Invoke-WebRequest -Uri 'https://raw.githubusercontent.com/steveloughran/winutils/master/hadoop-3.3.4/bin/hadoop.dll' -OutFile 'D:\deals\hadoop\bin\hadoop.dll'"
    
    echo    ✅ Hadoop файлы установлены
)

:: ========================================
:: 5. Настройка переменных окружения
:: ========================================
echo.
echo [6/7] Настройка переменных окружения...

:: Установка HADOOP_HOME
setx HADOOP_HOME "D:\deals\hadoop" /M > nul
echo    ✅ HADOOP_HOME установлен

:: Установка JAVA_HOME (проверяем существующие пути)
if exist "C:\Program Files\Eclipse Adoptium\jdk-8.0.482.8-hotspot" (
    setx JAVA_HOME "C:\Program Files\Eclipse Adoptium\jdk-8.0.482.8-hotspot" /M > nul
    echo    ✅ JAVA_HOME установлен (JDK 8)
) else if exist "C:\Program Files\Java\jdk1.8.0_*" (
    for /d %%i in ("C:\Program Files\Java\jdk1.8.0_*") do setx JAVA_HOME "%%i" /M > nul
    echo    ✅ JAVA_HOME установлен (Java 8)
) else (
    echo    ⚠️ JAVA не найдена. Установите JDK 8 вручную.
)

:: Установка PYSPARK_PYTHON
setx PYSPARK_PYTHON "C:\Python310\python.exe" /M > nul
setx PYSPARK_DRIVER_PYTHON "C:\Python310\python.exe" /M > nul
echo    ✅ PYSPARK_PYTHON установлен

:: ========================================
:: 6. Создание структуры проекта
:: ========================================
echo.
echo [7/7] Создание структуры проекта...

:: Создание необходимых папок
mkdir D:\tmp 2>nul
mkdir D:\tmp\spark-warehouse 2>nul
mkdir D:\tmp\metastore 2>nul
mkdir D:\deals\pyspark_utils\notebooks 2>nul
mkdir D:\deals\pyspark_utils\data 2>nul
mkdir D:\deals\pyspark_utils\scripts 2>nul

echo    ✅ Структура проекта создана

:: ========================================
:: 7. Создание тестового скрипта
:: ========================================
echo.
echo Создание тестового скрипта...

(
echo import os
echo import sys
echo from pyspark.sql import SparkSession
echo.
echo print("="*50)
echo print("ТЕСТОВЫЙ ЗАПУСК PYSPARK")
echo print("="*50)
echo.
echo # Настройка окружения
echo os.environ['HADOOP_HOME'] = 'D:\\deals\\hadoop'
echo os.environ['PYSPARK_PYTHON'] = 'C:\\Python310\\python.exe'
echo os.environ['PYSPARK_DRIVER_PYTHON'] = 'C:\\Python310\\python.exe'
echo.
echo print(f"Python: {sys.executable}")
echo print(f"Python версия: {sys.version}")
echo.
echo # Создание SparkSession
echo spark = SparkSession.builder ^
echo     .appName("TestInstall") ^
echo     .master("local[*]") ^
echo     .config("spark.driver.host", "127.0.0.1") ^
echo     .getOrCreate()
echo.
echo print(f"Spark версия: {spark.version}")
echo.
echo # Тестовые данные
echo data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
echo df = spark.createDataFrame(data, ["Name", "Age"])
echo.
echo print("\\nДанные:")
echo df.show()
echo.
echo print(f"Количество записей: {df.count()}")
echo.
echo # Сохранение тестовых данных
echo df.write.mode("overwrite").option("header", "true").csv("D:/tmp/test_output")
echo print("✅ Тестовые данные сохранены в D:/tmp/test_output")
echo.
echo spark.stop()
echo print("\\n✅ Тест завершен успешно!")
) > D:\deals\pyspark_utils\test_install.py

echo    ✅ Тестовый скрипт создан

:: ========================================
:: 8. Создание ярлыков для запуска
:: ========================================
echo.
echo Создание ярлыков для запуска...

:: Скрипт для запуска Jupyter
(
echo @echo off
echo cd /d D:\deals\pyspark_utils
echo C:\Python310\python.exe -m jupyter notebook
) > D:\deals\pyspark_utils\start_jupyter.bat

:: Скрипт для запуска VS Code
(
echo @echo off
echo cd /d D:\deals\pyspark_utils
echo code .
) > D:\deals\pyspark_utils\start_vscode.bat

:: Скрипт для запуска теста
(
echo @echo off
echo cd /d D:\deals\pyspark_utils
echo C:\Python310\python.exe test_install.py
echo pause
) > D:\deals\pyspark_utils\run_test.bat

echo    ✅ Ярлыки созданы

:: ========================================
:: 9. Итог
:: ========================================
echo.
echo ========================================
echo    УСТАНОВКА ЗАВЕРШЕНА!
echo ========================================
echo.
echo 📁 Проект: D:\deals\pyspark_utils
echo.
echo 📦 Установленные пакеты:
echo    - PySpark 3.3.4
echo    - Pandas, NumPy
echo    - Jupyter, Jupyter Lab
echo    - FindSpark, PyArrow
echo.
echo 🔧 Переменные окружения:
echo    - HADOOP_HOME = D:\deals\hadoop
echo    - PYSPARK_PYTHON = C:\Python310\python.exe
echo    - JAVA_HOME = (проверьте вручную)
echo.
echo 🚀 Как запустить:
echo    run_test.bat     - проверить установку
echo    start_jupyter.bat - запустить Jupyter
echo    start_vscode.bat  - открыть VS Code
echo.
echo ⚠️ ВАЖНО: Перезагрузите компьютер для применения переменных!
echo.
echo Нажмите Enter для выхода...
pause > nul