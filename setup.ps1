# setup.ps1
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "   УСТАНОВКА PYSPARK ПРОЕКТА" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Функция для проверки и установки
function Install-Package {
    param($PackageName, $InstallCommand)
    
    Write-Host "Проверка $PackageName..." -ForegroundColor Yellow
    try {
        $null = Get-Command $PackageName -ErrorAction SilentlyContinue
        Write-Host "   ✅ $PackageName уже установлен" -ForegroundColor Green
    } catch {
        Write-Host "   ⚠️ Установка $PackageName..." -ForegroundColor Yellow
        Invoke-Expression $InstallCommand
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   ✅ $PackageName установлен" -ForegroundColor Green
        } else {
            Write-Host "   ❌ Ошибка установки $PackageName" -ForegroundColor Red
        }
    }
}

# 1. Проверка Python
Write-Host "[1/6] Проверка Python 3.10..." -ForegroundColor Cyan
$pythonPath = "C:\Python310\python.exe"
if (Test-Path $pythonPath) {
    Write-Host "   ✅ Python 3.10 найден" -ForegroundColor Green
} else {
    Write-Host "   ⚠️ Установка Python 3.10..." -ForegroundColor Yellow
    winget install --id Python.Python.3.10 -e --source winget
}

# 2. Установка пакетов
Write-Host "`n[2/6] Установка Python пакетов..." -ForegroundColor Cyan

$packages = @(
    "pyspark==3.3.4",
    "pandas", "numpy",
    "jupyter", "notebook", "jupyterlab", "ipykernel",
    "jedi", "pyarrow", "findspark",
    "matplotlib", "seaborn", "scikit-learn"
)

foreach ($package in $packages) {
    Write-Host "   Установка $package..." -ForegroundColor Yellow
    & $pythonPath -m pip install $package
}

# 3. Создание Jupyter ядра
Write-Host "`n[3/6] Создание Jupyter ядра..." -ForegroundColor Cyan
& $pythonPath -m ipykernel install --user --name pyspark310 --display-name "Python (PySpark 3.10)"

# 4. Проверка Hadoop
Write-Host "`n[4/6] Проверка Hadoop..." -ForegroundColor Cyan
$hadoopPath = "D:\deals\hadoop\bin"
if (Test-Path "$hadoopPath\winutils.exe") {
    Write-Host "   ✅ Hadoop файлы найдены" -ForegroundColor Green
} else {
    Write-Host "   ⚠️ Скачивание Hadoop файлов..." -ForegroundColor Yellow
    New-Item -ItemType Directory -Force -Path $hadoopPath | Out-Null
    
    # Скачивание winutils.exe
    $winutilsUrl = "https://raw.githubusercontent.com/steveloughran/winutils/master/hadoop-3.3.4/bin/winutils.exe"
    Invoke-WebRequest -Uri $winutilsUrl -OutFile "$hadoopPath\winutils.exe"
    
    # Скачивание hadoop.dll
    $dllUrl = "https://raw.githubusercontent.com/steveloughran/winutils/master/hadoop-3.3.4/bin/hadoop.dll"
    Invoke-WebRequest -Uri $dllUrl -OutFile "$hadoopPath\hadoop.dll"
    
    Write-Host "   ✅ Hadoop файлы установлены" -ForegroundColor Green
}

# 5. Создание структуры
Write-Host "`n[5/6] Создание структуры проекта..." -ForegroundColor Cyan
$folders = @(
    "D:\tmp", "D:\tmp\spark-warehouse", "D:\tmp\metastore",
    "D:\deals\pyspark_utils\notebooks",
    "D:\deals\pyspark_utils\data",
    "D:\deals\pyspark_utils\scripts"
)

foreach ($folder in $folders) {
    New-Item -ItemType Directory -Force -Path $folder | Out-Null
}
Write-Host "   ✅ Структура создана" -ForegroundColor Green

# 6. Настройка переменных окружения
Write-Host "`n[6/6] Настройка переменных окружения..." -ForegroundColor Cyan

[Environment]::SetEnvironmentVariable("HADOOP_HOME", "D:\deals\hadoop", "Machine")
[Environment]::SetEnvironmentVariable("PYSPARK_PYTHON", "C:\Python310\python.exe", "Machine")
[Environment]::SetEnvironmentVariable("PYSPARK_DRIVER_PYTHON", "C:\Python310\python.exe", "Machine")

Write-Host "   ✅ Переменные установлены" -ForegroundColor Green
Write-Host "   ⚠️ Нужна перезагрузка для применения" -ForegroundColor Yellow

# Итог
Write-Host "`n========================================" -ForegroundColor Green
Write-Host "   УСТАНОВКА ЗАВЕРШЕНА!" -ForegroundColor Green
Write-Host "========================================" -ForegroundColor Green
Write-Host ""
Write-Host "Для запуска проекта:" -ForegroundColor Cyan
Write-Host "1. Перезагрузите компьютер"
Write-Host "2. Запустите D:\deals\pyspark_utils\run_test.bat"
Write-Host "3. Или откройте VS Code и выберите Python 3.10"
Write-Host ""

Read-Host "Нажмите Enter для выхода"