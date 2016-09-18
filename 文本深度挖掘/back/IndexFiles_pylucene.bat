d:
cd D:\TotalCode\LuceneCode\Index_Search
for /f "delims=" %%i in (D:\TotalCode\LuceneCode\GetData\RecentWeek.txt) do (
  python IndexFiles_pylucene.py D:\DATA\MBStrategy D:\DATA\Index\MBStrategy %%i
  python IndexFiles_pylucene.py D:\DATA\text D:\DATA\Index\text %%i
  python IndexFiles_pylucene.py D:\DATA\sinaStockNews D:\DATA\Index\sinaStockNews %%i
)