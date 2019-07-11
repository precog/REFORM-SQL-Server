Press the Start menu, type Powershell and press Enter.

Then drag the application REFORM to the Powershell window.

Then press space and paste in quotes the connection string for your SQL Server.

Then press space and type the name of your SQL Server's database that you want to push your data into (you may need to create this first).

Then press space and type the name of your SQL Server's schema that you want to push your data into (if you don't know what this is use "dbo").

Then press space and type no-count unless you need support for multibyte characters in which case type count.

Then press space and type "create" unless you would like to append or replace existing tables in which case type "append" or "create".

Then press space and type the name of the extra column such as "CreatedAt".

Then press space and type the extra column type such as "offsetdatetime", "number", "string" or "boolean".

Then press space and type the extra column value such as "2019-05-30T20:18:53Z".

Then if you want to transfer only a specific table go REFORM go to the virtual table listing, click the virtual table you want to load and copy the uuid from browser URL.

Or if you want to transfer all tables which haven't been archived go to REFORM and copy the URL with no trailing / e.g. https://reform.example.com

In the Powershell window press space and paste the REFORM access link.

Press enter.

Your data is now streaming from your data source via REFORM into a table in SQL Server.

The name of your table is taken from the name of the virtual table in REFORM.

Single table with current datetime example:

C:\Users\Administrator\Downloads\REFORM\REFORM.exe "Data Source=EC2AMAZ-PFM7ROF;Integrated Security=True;Persist Security Info=False;Pooling=False;MultipleActiveResultSets=False;Encrypt=False;TrustServerCertificate=False;" example_database dbo false create "CreatedAt" "offsetdatetime" $(Get-Date -Format "o") https://reform.example.com 431182ea-6b62-47d6-82da-a5f1d373c540

Single table with specified datetime example:

C:\Users\Administrator\Downloads\REFORM\REFORM.exe "Data Source=EC2AMAZ-PFM7ROF;Integrated Security=True;Persist Security Info=False;Pooling=False;MultipleActiveResultSets=False;Encrypt=False;TrustServerCertificate=False;" example_database dbo false create "CreatedAt" "offsetdatetime" "2019-05-30T20:18:53Z" https://reform.example.com
