import paramiko

host = "pplas-ldbr00412.sdpdi.df.sbrf.ru" # Замените на ваш сервер
user = "22343006_omega-sbrf-ru" # Замените на ваше имя пользователя
password = "$$" # Замените на ваш пароль
command = "cd dev/util_hive2ch;source generate_ddl.sh" # Команда для выполнения на сервере
filename = 'wf_rdwh_hive_to_ch__template_dag_v10.py'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

remote_file_path = f'oozie-app/oozie-app/util_hive2ch' # в пути нельзя использовать тильду
local_file_path = 'file.txt'

ssh.connect(host, username=user, password=password)

sftp = ssh.open_sftp()
sftp.get(remote_file_path, local_file_path)
sftp.close()

# try:
#     ssh.connect(host, username=user, password=password)
#     stdin, stdout, stderr = ssh.exec_command(command)
#     output = stdout.read().decode()
#     error = stderr.read().decode()
#     if output:
#         print("Результат выполнения команды:")
#         print(output)
#     if error:
#         print("Ошибка:")
#         print(error)
# except Exception as e:
#     print(f"Ошибка подключения: {e}")
# finally:
#     ssh.close()