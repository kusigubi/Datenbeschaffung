$LogTime = Get-Date -Format "MM-dd-yyyy_hh-mm-ss"
$LogFile = "E:/Datenbeschaffung/script_log.txt"
"Start $LogTime"| Out-File $LogFile -Append -Force
echo $env:Path | Out-File $LogFile -Append -Force
python "E:/Datenbeschaffung/Aggregation_v1.91.py" 
$LogTime = Get-Date -Format "MM-dd-yyyy_hh-mm-ss"
"End $LogTime"| Out-File $LogFile -Append -Force