@echo off
chcp 65001 > nul
echo ========================================
echo    PySpark - Быстрый запуск
echo ========================================
echo.

:: Установка переменных для текущей сессии
set HADOOP_HOME=D:\deals\hadoop
set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-8.0.482.8-hotspot
set PYSPARK_PYTHON=C:\Python310\python.exe
set PYSPARK_DRIVER_PYTHON=C:\Python310\python.exe

:: Добавляем в PATH
set PATH=%HADOOP_HOME%\bin;%JAVA_HOME%\bin;C:\Windows\System32;%PATH%

:: Проверка версий
echo [1/4] Проверка Java...
java -version 2>&1 | find "version"
if %errorlevel% equ 0 (
    echo    ✅ Java найдена
) else (
    echo    ❌ Java не найдена
)

echo.
echo [2/4] Проверка Python...
python --version
if %errorlevel% equ 0 (
    echo    ✅ Python найден
) else (
    echo    ❌ Python не найден
)

echo.
echo [3/4] Проверка Hadoop...
if exist %HADOOP_HOME%\bin\winutils.exe (
    echo    ✅ winutils.exe найден
    :: Проверяем работу winutils
    %HADOOP_HOME%\bin\winutils.exe ls \ 2>nul >nul
    if %errorlevel% equ 0 (
        echo    ✅ winutils работает
    ) else (
        echo    ⚠️ winutils есть, но могут быть проблемы с правами
    )
) else (
    echo    ❌ winutils.exe НЕ найден
    echo    Проверьте путь: %HADOOP_HOME%\bin\winutils.exe
)

echo.
echo [4/4] Запуск Jupyter...
echo.

:: Проверяем наличие jupyter
where jupyter >nul 2>nul
if %errorlevel% equ 0 (
    echo ✅ Jupyter найден, запускаем...
    cd /d D:\deals\pyspark_utils
    jupyter notebook
) else (
    echo ❌ Jupyter не найден. Устанавливаем...
    pip install jupyter notebook
    if %errorlevel% equ 0 (
        echo ✅ Jupyter установлен, запускаем...
        jupyter notebook
    ) else (
        echo ❌ Не удалось установить Jupyter
        pause
        exit /b 1
    )
)

pause