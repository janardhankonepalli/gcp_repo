./daily_report.sh > file.tmp
mailx -s "Daily Report" dpjobscheduler@gmail.com <file.tmp
rm file.tmp
