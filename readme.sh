# if you want inspect the contents of BSON file, you can use `bsondump`
bsondump transactions.bson

#if you want restore the db, use `mongostore`
mongostore --db yournewdatabase dump/yourdatabase

#run
go run .\agregation\agregation.go