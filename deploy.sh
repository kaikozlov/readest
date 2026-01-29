ssh root@100.69.181.93 -p 5132 "rm -rf /mnt/us/koreader/plugins/readest.koplugin /mnt/us/koreader/crash.log" && \
scp -P 5132 -r ./apps/readest.koplugin root@100.69.181.93:/mnt/us/koreader/plugins/
