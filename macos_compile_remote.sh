#!/bin/bash
export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8
version=$1

# clean old binary zip file
cd /Users/kay/Documents/GoProj/OracleSync2MySQL/binary
rm -rf OracleSync2MySQL.exe
rm -rf example.yml
rm -rf OracleSync2MySQL-MacOS-x64-v$version.zip
rm -rf OracleSync2MySQL-win-x64-v$version.zip
rm -rf OracleSync2MySQL-linux-x64-v$version.zip

# MacOS macos compile
cd /Users/kay/Documents/GoProj/OracleSync2MySQL
/Users/kay/sdk/go1.22.0/bin/go clean
/Users/kay/sdk/go1.22.0/bin/go build -o OracleSync2MySQL OracleSync2MySQL
zip -r OracleSync2MySQL-MacOS-x64-v$version.zip OracleSync2MySQL example.yml instantclient
mv OracleSync2MySQL-MacOS-x64-v$version.zip binary


# Linux ssh remote linux server and compile
ssh root@192.168.74.129 "rm -rf /root/go/src/OracleSync2MySQL/cmd"
ssh root@192.168.74.129 "rm -rf /root/go/src/OracleSync2MySQL/connect"
ssh root@192.168.74.129 "rm -rf /root/go/src/OracleSync2MySQL/go.mod"
ssh root@192.168.74.129 "rm -rf /root/go/src/OracleSync2MySQL/go.sum"
ssh root@192.168.74.129 "rm -rf /root/go/src/OracleSync2MySQL/main.go"
ssh root@192.168.74.129 "rm -rf /root/go/src/OracleSync2MySQL/*.yml"
ssh root@192.168.74.129 "rm -rf /root/go/src/OracleSync2MySQL/*.zip"
scp -r /Users/kay/Documents/GoProj/OracleSync2MySQL/cmd/ root@192.168.74.129:/root/go/src/OracleSync2MySQL
scp -r /Users/kay/Documents/GoProj/OracleSync2MySQL/connect/ root@192.168.74.129:/root/go/src/OracleSync2MySQL
scp  /Users/kay/Documents/GoProj/OracleSync2MySQL/go.mod root@192.168.74.129:/root/go/src/OracleSync2MySQL
scp  /Users/kay/Documents/GoProj/OracleSync2MySQL/go.sum root@192.168.74.129:/root/go/src/OracleSync2MySQL
scp /Users/kay/Documents/GoProj/OracleSync2MySQL/main.go root@192.168.74.129:/root/go/src/OracleSync2MySQL
scp /Users/kay/Documents/GoProj/OracleSync2MySQL/*.yml root@192.168.74.129:/root/go/src/OracleSync2MySQL
ssh root@192.168.74.129 "rm -rf /root/go/src/OracleSync2MySQL/*.zip"
ssh root@192.168.74.129 "cd /root/go/src/OracleSync2MySQL && /usr/local/go/bin/go clean"
ssh root@192.168.74.129 "cd /root/go/src/OracleSync2MySQL && /usr/local/go/bin/go build -o OracleSync2MySQL OracleSync2MySQL"
ssh root@192.168.74.129 "cd /root/go/src/OracleSync2MySQL && zip -r OracleSync2MySQL-linux-x64-v$version.zip OracleSync2MySQL example.yml instantclient"
scp root@192.168.74.129:/root/go/src/OracleSync2MySQL/OracleSync2MySQL-linux-x64-v$version.zip /Users/kay/Documents/GoProj/OracleSync2MySQL/binary


# Windows ssh remote Windows server and compile
ssh administrator@192.168.149.80 "cd C:\go\src\OracleSync2MySQL && C:\Users\Administrator\sdk\go1.20.6\bin\go clean"
ssh administrator@192.168.149.80 "cd C:\go\src\OracleSync2MySQL && del /f /s /q *.zip *.yml go* main.go cmd connect"
scp -r /Users/kay/Documents/GoProj/OracleSync2MySQL/cmd/ administrator@192.168.149.80:"C:\go\src\OracleSync2MySQL"
scp -r /Users/kay/Documents/GoProj/OracleSync2MySQL/connect/ administrator@192.168.149.80:"C:\go\src\OracleSync2MySQL"
scp -r /Users/kay/Documents/GoProj/OracleSync2MySQL/test/ administrator@192.168.149.80:"C:\go\src\OracleSync2MySQL"
scp  /Users/kay/Documents/GoProj/OracleSync2MySQL/go.mod administrator@192.168.149.80:"C:\go\src\OracleSync2MySQL"
scp  /Users/kay/Documents/GoProj/OracleSync2MySQL/go.sum administrator@192.168.149.80:"C:\go\src\OracleSync2MySQL"
scp /Users/kay/Documents/GoProj/OracleSync2MySQL/main.go administrator@192.168.149.80:"C:\go\src\OracleSync2MySQL"
scp /Users/kay/Documents/GoProj/OracleSync2MySQL/*.yml administrator@192.168.149.80:"C:\go\src\OracleSync2MySQL"
ssh administrator@192.168.149.80 "cd C:\go\src\OracleSync2MySQL && C:\Users\Administrator\sdk\go1.20.6\bin\go build -o OracleSync2MySQL.exe OracleSync2MySQL"
# pack zip file from windows to local mac
cd /Users/kay/Documents/GoProj/OracleSync2MySQL/binary
scp administrator@192.168.149.80:"C:/go/src/OracleSync2MySQL/OracleSync2MySQL.exe" /Users/kay/Documents/GoProj/OracleSync2MySQL/binary
cp /Users/kay/Documents/GoProj/OracleSync2MySQL/example.yml .
zip -r OracleSync2MySQL-win-x64-v$version.zip OracleSync2MySQL.exe example.yml instantclient