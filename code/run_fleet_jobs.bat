@echo off

cd /d "C:\Users\Admin\OneDrive\Operational Intelligence Hub\code"

echo ============================== >> log.txt
echo Running jobs at %date% %time% >> log.txt

python RevenueConcat.py >> log.txt 2>&1
python MileageConcats.py >> log.txt 2>&1
python CleanedEcharge.py >> log.txt 2>&1


echo Completed at %date% %time% >> log.txt