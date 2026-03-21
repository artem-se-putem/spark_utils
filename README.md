Все команды выполнять в vs code запущенном с правами админа

# Установить Scoop в powerShell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
Invoke-RestMethod -Uri https://get.scoop.sh | Invoke-Expression

# Установить make
scoop install make

cd docker_start

make up

make down