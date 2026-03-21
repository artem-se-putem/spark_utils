@echo off
chcp 65001 > nul
title Установка PySpark проекта
color 0A

echo ========================================
echo    УСТАНОВКА PYSPARK ПРОЕКТА
echo ========================================
echo.
echo Проект: C:\deals\pyspark_utils
echo Дата: %date%
echo.
echo Нажмите Enter для начала установки...
pause > nul

:: ========================================
:: 0. НАСТРОЙКА ПЕРЕМЕННЫХ ОКРУЖЕНИЯ (ВСЕ В ОДНОМ МЕСТЕ)
:: ========================================
echo.
echo [0/7] Настройка переменных окружения...

:: Python пути
set "PYTHON_PATH=C:\Users\artem\AppData\Local\Programs\Python\Python310\python.exe"
set "PYSPARK_PYTHON=C:\Users\artem\AppData\Local\Programs\Python\Python310\python.exe"
set "PYSPARK_DRIVER_PYTHON=C:\Users\artem\AppData\Local\Programs\Python\Python310\python.exe"

:: Hadoop путь
set "HADOOP_HOME=C:\deals\hadoop"

:: Java путь
set "JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-8.0.482.8-hotspot"

:: Установка постоянных переменных (setx)
setx PYTHON_PATH "%PYTHON_PATH%" /M > nul
setx PYSPARK_PYTHON "%PYSPARK_PYTHON%" /M > nul
setx PYSPARK_DRIVER_PYTHON "%PYSPARK_DRIVER_PYTHON%" /M > nul
setx HADOOP_HOME "%HADOOP_HOME%" /M > nul
setx JAVA_HOME "%JAVA_HOME%" /M > nul

echo    ✅ Переменные окружения настроены
echo    ✅ PYTHON_PATH = %PYTHON_PATH%
echo    ✅ PYSPARK_PYTHON = %PYSPARK_PYTHON%
echo    ✅ PYSPARK_DRIVER_PYTHON = %PYSPARK_DRIVER_PYTHON%
echo    ✅ HADOOP_HOME = %HADOOP_HOME%
echo    ✅ JAVA_HOME = %JAVA_HOME%

:: ========================================
:: 1. Проверка Python
:: ========================================
echo.
echo [1/7] Проверка Python 3.10...
if exist "%PYTHON_PATH%" (
    echo    ✅ Python 3.10 найден
) else (
    echo    ❌ Python 3.10 не найден
    echo    Установка Python 3.10...
    winget install --id Python.Python.3.10 -e --source winget
    if %errorlevel% equ 0 (
        echo    ✅ Python 3.10 установлен
    ) else (
        echo    ❌ Ошибка установки Python
        echo    Скачайте вручную: https://www.python.org/downloads/release/python-31010/
        pause
        exit /b 1
    )
)

:: ========================================
:: 2. Проверка pip
:: ========================================
echo.
echo [2/7] Обновление pip...
"%PYTHON_PATH%" -m pip install --upgrade pip
echo    ✅ pip обновлен

:: ========================================
:: 3. Установка необходимых пакетов
:: ========================================
echo.
echo [3/7] Установка Python пакетов...

"%PYTHON_PATH%" -m pip install pyspark==3.3.4
echo    ✅ PySpark 3.3.4 установлен

"%PYTHON_PATH%" -m pip install pandas
echo    ✅ Pandas установлен

"%PYTHON_PATH%" -m pip install numpy
echo    ✅ NumPy установлен

"%PYTHON_PATH%" -m pip install jupyter notebook
echo    ✅ Jupyter установлен

"%PYTHON_PATH%" -m pip install jupyterlab
echo    ✅ Jupyter Lab установлен

"%PYTHON_PATH%" -m pip install ipykernel
echo    ✅ IPython kernel установлен

"%PYTHON_PATH%" -m pip install jedi
echo    ✅ Jedi установлен

"%PYTHON_PATH%" -m pip install pyarrow
echo    ✅ PyArrow установлен

"%PYTHON_PATH%" -m pip install findspark
echo    ✅ FindSpark установлен

:: Создание ядра для Jupyter
echo.
echo [4/7] Создание Jupyter ядра...
"%PYTHON_PATH%" -m ipykernel install --user --name pyspark310 --display-name "Python (PySpark 3.10)"
echo    ✅ Ядро pyspark310 создано

:: ========================================
:: 4. Проверка Hadoop
:: ========================================
echo.
echo [5/7] Проверка Hadoop...
if exist "%HADOOP_HOME%\bin\winutils.exe" (
    echo    ✅ Hadoop файлы найдены
) else (
    echo    ⚠️ Hadoop файлы не найдены
    echo    Создание папки Hadoop...
    mkdir "%HADOOP_HOME%\bin" 2>nul
    
    echo    Скачивание winutils.exe...
    powershell -Command "Invoke-WebRequest -Uri 'https://raw.githubusercontent.com/steveloughran/winutils/master/hadoop-3.3.4/bin/winutils.exe' -OutFile '%HADOOP_HOME%\bin\winutils.exe'"
    
    echo    Скачивание hadoop.dll...
    powershell -Command "Invoke-WebRequest -Uri 'https://raw.githubusercontent.com/steveloughran/winutils/master/hadoop-3.3.4/bin/hadoop.dll' -OutFile '%HADOOP_HOME%\bin\hadoop.dll'"
    
    echo    ✅ Hadoop файлы установлены
)

:: ========================================
:: 5. Проверка Java
:: ========================================
echo.
echo [6/7] Проверка Java 8...
if exist "%JAVA_HOME%" (
    echo    ✅ Java 8 найден: %JAVA_HOME%
) else (
    echo    ⚠️ Java 8 не найден по пути %JAVA_HOME%
    echo    Проверка альтернативных путей...
    if exist "C:\Program Files\Java\jdk1.8.0_*" (
        for /d %%i in ("C:\Program Files\Java\jdk1.8.0_*") do (
            set "JAVA_HOME=%%i"
            setx JAVA_HOME "%%i" /M > nul
            echo    ✅ JAVA_HOME установлен: %%i
        )
    ) else (
        echo    ⚠️ JAVA не найдена. Установите JDK 8 вручную.
        echo    Скачайте: https://adoptium.net/temurin/releases/?version=8
    )
)

:: ========================================
:: 6. Итог
:: ========================================
echo.
echo ========================================
echo    УСТАНОВКА ЗАВЕРШЕНА!
echo ========================================
echo.
echo 📁 Проект: C:\deals\pyspark_utils
echo.
echo 📦 Установленные пакеты:
echo    - PySpark 3.3.4
echo    - Pandas, NumPy
echo    - Jupyter, Jupyter Lab
echo    - FindSpark, PyArrow
echo.
echo 🔧 Переменные окружения:
echo    - HADOOP_HOME = %HADOOP_HOME%
echo    - PYSPARK_PYTHON = %PYSPARK_PYTHON%
echo    - JAVA_HOME = %JAVA_HOME%
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