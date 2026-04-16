# Утилита для автоматизации репликации из hive в clickhouse с поддержкой инкрементальной загрузки по дате

## Как пользоваться инструкцией:

План инструкции:

1. Пререквизиты:
    - необходимо убедиться, что все необходимые доступы у вас есть
2. Установка:
    - Настройка подключения к удаленной linux-ноде из Pycharm ВАРМа
    - Настройка Browse Remote Host для просмотра файлов на linux-ноде, находясь в ВАРМе
    - Клонирование репозитория на linux-ноду и переход в корень приложения
3. Настройка параметров приложения:
    - ОБЩИЕ ПАРАМЕТРЫ
    - ПАРАМЕТРЫ ПРИЛОЖЕНИЯ (для справки по параметрам приложения обратитесь к разделу 5. СПРАВКА: Параметры приложения)
    - ПАРАМЕТРЫ SPARK SESSION
4. Сделайте кейтаб. Дальше в зависимости от того, хотите вы запускать приложение ЛОКАЛЬНО, через CTL OOZIE или через AIRFLOW, переходите в соответствующий раздел:

   4.1. Локальный запуск приложения на linux-ноде

   4.2. Ставим приложение на рассписание с помощью OOZIE в CTL

   4.3. Как поставить приложение на рассписание в airflow с помощью SparkSubmitOperator
5. СПРАВКА: Параметры приложения
6. Справочная информация для debugging

### Пошаговая схема:

Цветовая палитра схемы:
- Серый - выполняется в консоли Linux
- Красный - ручное редактирование
- Синий - действия в UI приложений

![img35.png](docs/img35.png)


## 1. Пререквизиты
1. доступ к linux-ноде с доступом к КП (hdfs)
2. доступ к командное пространство (hdfs)
3. доступ к airflowSE
4. доступ к ЛД Clickhouse
5. доступ к командной папке в hdfs (где вы будете хранить это приложение)

## 2. Установка
### Настраиваем подключение к удаленной linux-ноде в Pycharm. На неё будем клонировать приложение. А также настраиваем Remote Browser Host для просмотра файлов на linux-ноде
1. Help->Find Action

![img17.png](docs/img17.png)

2. Вводим в поле поиска "ssh configurations"

![img18.png](docs/img18.png)

3. Заполняем поля по примеру и жмём Apply:

Можете использовать этот хост или свой:
```bash
Host: pplas-ldbr00412.sdpdi.df.sbrf.ru
```

![img19.png](docs/img19.png)

4. Tools->Deployment->Configuration...

![img22.png](docs/img22.png)

5. Жмём "+", вводим название конфигурации. Далее создаем конфигурацию для Deployment, выбрав вашу конфигурацию SSH и указав Root path:

![img24.png](docs/img24.png)

6. Открываете Browse Remote Host:

![img25.png](docs/img25.png)

7. Tools->Start SSH Session... и выбираете ваше подключение

![img20.png](docs/img20.png)

8. В терминале вы подключились к удаленной linux-ноде

![img21.png](docs/img21.png)

### Клонируем репозиторий и переходим в папку проекта
1. В терминале linux-ноды клонируем репозиторий
```bash
git clone https://df-bitbucket.ca.sbrf.ru/scm/llrr/ld-sdp-etl.git
```
2. Переходим в репозиторий
```bash
cd util_hive2ch
```
3. В окне Browse Remote Host нажмите кнопку Refresh, чтобы увидеть обновленные файлы с linux-ноды.
   Перейдите к корню приложения:

![img33.png](docs/img33.png)

## 3. Настройка параметров приложения
В данном разделе редактируются параметры только в файле common.env.
После того, как измените файл common.env в Browser Remote Host необходимо нажать на кнопку загрузки на хост (правый верхний угол).

![img27.png](docs/img27.png)

1. В common.env заполнить "ОБЩИЕ ПАРАМЕТРЫ":

![img9.png](docs/img9.png)

2. Затем заполнить "ПАРАМЕТРЫ ПРИЛОЖЕНИЯ"

   (Если у вас возникают вопросы как заполнить параметры, обратитесь к разделу инструкции "СПРАВКА: Параметры приложения", можно оставить как есть для тестового запуска)

![img12.png](docs/img12.png)

3. Заполнить переменные "ПАРАМЕТРЫ SPARK SESSION" (можно оставить как есть для тестового запуска):

![img26.png](docs/img26.png)

## 4. Сделайте кейтаб. Дальше в зависимости от того, хотите вы запускать приложение ЛОКАЛЬНО, через CTL OOZIE или через AIRFLOW, переходите в соответствующий раздел:

0. Сделайте keytab в папке ~/.auth/
```bash
make keytab_init
```
### 4.1 Локальный запуск приложения на linux-ноде

1. Выполнить команду:
```bash
make project_local-build
```
В результаты выполнения этой команды появляется следующие файлы:
- Файл для локального запуска приложения:
    - local_start.sh
- Файл ddl:
    - sql/{CH_DISTRIBUTED_TABLE}.sql
- Файл jobs:
    - sql/{JOB_NAME}.py
- [НЕ МЕНЯТЬ] Технические файлы:
    - creds.py
    - toolbox/generate_ddl.sh
2. Обновите файлы в Browse Remote Host
3. Откройте сгенерированный sql/{CH_DISTRIBUTED_TABLE}.sql:
    - Проверьте его и отредактируйте ORDER BY и PARTITION BY у таблицы MergeTree, следуя рекомендациям из файла.
    - Создайте две таблицы: MergeTree и Distributed из этого файла.

4. Чтобы запустить загрузку из Hive в Clickhouse, выполните:
```bash
make local_start
```

5. Если увидите в консоли такие логи, то всё работает:

![img.png](docs/img.png)

Готово! Вы справились с **локальным** запуском приложения!
Чтобы остановить загрузку, нажмите `ctrl+C`

### 4.2 Ставим приложение на рассписание с помощью OOZIE в CTL:
1. В файле common.env заполнить параметры:
   ![img28.png](docs/img28.png)

2. Подготовить файлы приложение для oozie:

```bash
make project_oozie-build
```

3. Загрузить необходимые файлы в hdfs:

```bash
make project_load_oozie
```

В папке HDFS_APP_OOZIE_PATH появится файлы приложения необходимые для запуска потока oozie через CTL:

![img8.png](docs/img8.png)

4. Далее заходим в CTL и создаем CTL поток с параметрами:

![img7.png](docs/img7.png)

Готово! Можно запускать поток.


### 4.3 Как поставить приложение на рассписание в airflow с помощью SparkSubmitOperator:
1. Заполнить параметры:
   ![img16.png](docs/img16.png)

2. Подготовить файлы приложение для airflow:

```bash
make project_airflow-build
```

3. Загрузить файлы в hdfs:

```bash
make project_load_airflow
```

Эта команда сформирует необходимые файлы в hdfs по пути HDFS_APP_AIRFLOW_PATH, в том числе создаст DAG для airflow под ваши параметры.

![img30.png](docs/img30.png)


4. Откройте файл DAG по пути dags/{distributed_table}.py.

Этот даг нужно будет закинуть в AIRFLOW:

![img29.png](docs/img29.png)

Ссылка на Code Editor SE:
```https://airflow-webserver-ci04459811-airflow-app-os.apps.prom-terra000166-ias.ocp.ca.sbrf.ru/code_editor/#/files/```

Готово! можно запускать ваш DAG в AIRFLOW!

## 5. СПРАВКА: Параметры приложения:
Описание всех параметров приложения (чтобы знать как их сочетать, смотри след. подраздел "Комбинации параметров приложения").

Параметры указаны в формате:

```{параметр} [список возможных значений] - пояснение```

- increment [0,1,2,3] - режим загрузки данных (в качестве поля инкремента используется поле-партицирования из параметра sharding_key_ch):
    - 0 - историческая (с предварительным truncate таблицы в кликхаусе)
    - 1 - инкрементальная с save_interval (загрузка данных по отрезку времени [datetime.today() - save_interval,datetime.today()], с автоматической предварительной очисткой данных на этом отрезке)
    - 2 - инкрементальная загрузка по отрезку времени [start_date, end_date] с автоматической предварительной очисткой данных на этом отрезке
    - 3 - инкрементальная загрузка по отрезку времени [start_date, end_date] без автоматической предварительной очистки данных на этом отрезке

- cluster [sh8_r1] - имя логического кластера в кликхаусе
- schema_ch [rrb_ext] - схема таблицы в кликхаусе
- distributed_table [t_cred_portf_metric_mon_appl_tmp2_load_test] - имя distributed таблицы в кликхаусе, сюда будут грузиться данные
- replicated_table [t_cred_portf_metric_mon_appl_tmp2_mergetree_load_test] - имя mergetree таблицы, над которой построена distributed, необходимой для дропа партиций при increment =1 или =2
- sharding_key_ch [gregor_dt] - поддерживается только тип date, иначе не будут дропаться партиции, но данные грузиться в кликхаус из hive будут
- schema_hive [t_team_cpu_reporting_rrm] - схема источника данных в hive
- hive_table [t_cred_portf_metric_mon_appl_tmp2] - имя таблицы в hive

### Комбинации параметров приложения:
Параметры указываются в Makefile, для последующей генерации файла main_local_start.sh:

- (increment=0) для исторической загрузки:
```bash
INCREMENT='0'
JOB_NAME='t_cred_portf_metric_mon_appl_tmp2_load_test'
CH_SCHEMA='rrb_ext'
CH_DISTRIBUTED_TABLE='t_cred_portf_metric_mon_appl_tmp2_load_test'
CH_MERGETREE_TABLE='t_cred_portf_metric_mon_appl_tmp2_mergetree_load_test'
HIVE_SCHEMA='t_team_cpu_reporting_rrm'
HIVE_TABLE='t_cred_portf_metric_mon_appl_tmp2'
CH_PARTITION_BY_FIELD='gregor_dt'
CH_CLUSTER='sh8_r1'
```

-  (increment=1)для инкрементальной загрузки с save_interval:
```bash
INCREMENT='1'
SAVE_INTERVAL='5'
JOB_NAME='t_cred_portf_metric_mon_appl_tmp2_load_test'
CH_SCHEMA='rrb_ext'
CH_DISTRIBUTED_TABLE='t_cred_portf_metric_mon_appl_tmp2_load_test'
CH_MERGETREE_TABLE='t_cred_portf_metric_mon_appl_tmp2_mergetree_load_test'
HIVE_SCHEMA='t_team_cpu_reporting_rrm'
HIVE_TABLE='t_cred_portf_metric_mon_appl_tmp2'
CH_PARTITION_BY_FIELD='gregor_dt'
CH_CLUSTER='sh8_r1'
```

- (increment=2) для инкрементальной загрузки по отрезку времени [start_date, end_date]:
```bash
INCREMENT='2'
START_DATE='2025-05-05'
END_DATE='2025-05-05'
JOB_NAME='t_cred_portf_metric_mon_appl_tmp2_load_test'
CH_SCHEMA='rrb_ext'
CH_DISTRIBUTED_TABLE='t_cred_portf_metric_mon_appl_tmp2_load_test'
CH_MERGETREE_TABLE='t_cred_portf_metric_mon_appl_tmp2_mergetree_load_test'
HIVE_SCHEMA='t_team_cpu_reporting_rrm'
HIVE_TABLE='t_cred_portf_metric_mon_appl_tmp2'
CH_PARTITION_BY_FIELD='gregor_dt'
CH_CLUSTER='sh8_r1'
```

- (increment='3') для инкрементальной загрузки по отрезку времени [start_date, end_date], без предварительной очистки партиций:
```bash
INCREMENT='3'
START_DATE='2025-05-05'
END_DATE='2025-05-05'
JOB_NAME='t_cred_portf_metric_mon_appl_tmp2_load_test'
CH_SCHEMA='rrb_ext'
CH_DISTRIBUTED_TABLE='t_cred_portf_metric_mon_appl_tmp2_load_test'
CH_MERGETREE_TABLE='t_cred_portf_metric_mon_appl_tmp2_mergetree_load_test'
HIVE_SCHEMA='t_team_cpu_reporting_rrm'
HIVE_TABLE='t_cred_portf_metric_mon_appl_tmp2'
CH_PARTITION_BY_FIELD='gregor_dt'
CH_CLUSTER='sh8_r1'
```

## 6. Справочная информация для debugging
1. Чтобы посмотреть логи запущенного приложения в AIRFLOW, открываем вкладку logs конкретной таски и ищем синюю ссылку.

![img2.png](docs/img2.png)

2. Переходим и жмем внизу кнопку logs.

![img3.png](docs/img3.png)

3. Затем stdout.

![img4.png](docs/img4.png)

4. Это логи на кокретной ноде-координаторе, где запустилось ваше приложение.

5. Если вам нужно выключить загрузку, то необходимо остановить даг Mark state as... -> Failed.
   Затем два варианта
    1. перейти по синей ссылке в логах в SparkUI и нажать на kill каждой Spark таске.
    2. в hue перейти во вкладку jobs, выбрать вашу джобу и нажать кнопку kill.
