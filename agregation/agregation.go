package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(context.Background())

	collection := client.Database("blogs").Collection("blog_entries")

	fmt.Println("MatchGroupSort:")
	MatchGroupSort(collection)

	collectionOrders := client.Database("blogs").Collection("orders")
	fmt.Println("MatchGroupSortAdvance:")
	MatchGroupSortAdvance(collectionOrders)

	fmt.Println("MatchGroupGroupSort:")
	MatchGroupHavingSort(collectionOrders)

	collectionTransactions := client.Database("blogs").Collection("transactions")
	fmt.Println("IncomeExpense:")
	IncomeExpense(collectionTransactions)
}

func MatchGroupSort(collection *mongo.Collection) {

	// match = where
	// group = group
	// pipeline := []bson.M{
	// 	{
	// 		"$match": bson.M{
	// 			"author": "John",
	// 		},
	// 	},
	// 	{
	// 		"$group": bson.M{
	// 			"_id":   "$author",
	// 			"count": bson.M{"$sum": 1},
	// 		},
	// 	},
	// }

	// group = group
	// sort = order by
	pipeline := []bson.M{
		{
			"$group": bson.M{
				"_id":           "$author",
				"total_entries": bson.M{"$sum": 1},
			},
		},
		{
			"$sort": bson.M{
				"total_entries": -1,
			},
		},
	}

	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(context.Background())

	for cursor.Next(context.Background()) {
		var result bson.M
		if err := cursor.Decode(&result); err != nil {
			log.Fatal(err)
		}
		fmt.Println(result)
	}

}

func MatchGroupSortAdvance(collection *mongo.Collection) {
	startOfDay := time.Date(2023, 9, 30, 0, 0, 0, 0, time.UTC)          // Tanggal awal hari
	endOfDay := time.Date(2023, 9, 30, 23, 59, 59, 999999999, time.UTC) // Tanggal akhir hari

	pipeline := []bson.M{
		{
			"$match": bson.M{
				"orderDate": bson.M{
					"$gte": startOfDay, // Tanggal awal rentang waktu
					"$lte": endOfDay,   // Tanggal akhir rentang waktu
				},
			},
		},
		{
			"$group": bson.M{
				"_id":         bson.M{"$dateToString": bson.M{"format": "%Y-%m-%d", "date": "$orderDate"}},
				"totalAmount": bson.M{"$sum": "$totalAmount"},
			},
		},
		{
			"$sort": bson.M{"_id": 1}, // Urutkan berdasarkan tanggal
		},
	}

	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(context.Background())

	var results []bson.M
	if err := cursor.All(context.Background(), &results); err != nil {
		log.Fatal(err)
	}

	for _, result := range results {
		date := result["_id"].(string)
		totalAmount := result["totalAmount"].(int32)
		fmt.Printf("Tanggal: %s, Total Penjualan: %d\n", date, totalAmount)
	}

}

func MatchGroupHavingSort(collection *mongo.Collection) {
	startOfDay := time.Date(2023, 9, 01, 0, 0, 0, 0, time.UTC)          // Tanggal awal hari
	endOfDay := time.Date(2023, 9, 30, 23, 59, 59, 999999999, time.UTC) // Tanggal akhir hari

	pipeline := []bson.M{
		{
			"$match": bson.M{
				"orderDate": bson.M{
					"$gte": startOfDay,
					"$lte": endOfDay,
				},
			},
		},
		{
			"$group": bson.M{
				"_id":         bson.M{"$dateToString": bson.M{"format": "%Y-%m-%d", "date": "$orderDate"}},
				"totalAmount": bson.M{"$sum": "$totalAmount"},
			},
		},
		{
			"$match": bson.M{
				"totalAmount": bson.M{"$gte": 100}, // Contoh filter dengan jumlah total minimal 100
			},
		},
		{
			"$sort": bson.M{"_id": 1},
		},
	}

	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(context.Background())

	var results []bson.M
	if err := cursor.All(context.Background(), &results); err != nil {
		log.Fatal(err)
	}

	for _, result := range results {
		date := result["_id"].(string)
		totalAmount := result["totalAmount"].(int32)
		fmt.Printf("Tanggal: %s, Total Penjualan: %d\n", date, totalAmount)
	}
}

func IncomeExpense(collection *mongo.Collection) {
	pipeline := []bson.M{
		{
			"$addFields": bson.M{
				"transactionDate": bson.M{"$toDate": "$transactionDate"},
				"isExpense":       bson.M{"$eq": []interface{}{"$isIncome", false}},
			},
		},
		{
			"$group": bson.M{
				"_id": bson.M{
					"year":  bson.M{"$year": "$transactionDate"},
					"month": bson.M{"$month": "$transactionDate"},
				},
				"totalIncome":  bson.M{"$sum": bson.M{"$cond": []interface{}{"$isIncome", "$amount", 0}}},
				"totalExpense": bson.M{"$sum": bson.M{"$cond": []interface{}{"$isExpense", "$amount", 0}}},
			},
		},
		{
			"$sort": bson.M{"_id.year": 1, "_id.month": 1},
		},
	}

	cursor, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(context.Background())

	var results []bson.M
	if err := cursor.All(context.Background(), &results); err != nil {
		log.Fatal(err)
	}

	for _, result := range results {
		year := result["_id"].(bson.M)["year"].(int32)
		month := result["_id"].(bson.M)["month"].(int32)
		totalIncome := result["totalIncome"].(int32)
		totalExpense := result["totalExpense"].(int32)

		fmt.Printf("Laporan Keuangan Bulan %d-%02d\n", year, month)
		fmt.Printf("Total Pendapatan: %d\n", totalIncome)
		fmt.Printf("Total Pengeluaran: %d\n", totalExpense)
		fmt.Printf("Saldo Bersih: %d\n", totalIncome-totalExpense)
		fmt.Println("-----------------------------")
	}

}
