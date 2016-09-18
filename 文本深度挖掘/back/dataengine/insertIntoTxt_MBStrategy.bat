D:
cd D:\TotalCode\LuceneCode\GetData
for /f "delims=" %%i in (RecentWeek.txt) do (
  python insertIntoTxt_MBStrategy.py %%i
)
pause