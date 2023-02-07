package main

import (
	"fmt"

	"github.com/MomirMilutinovic/NAiSP_Projekat/engine"
)

func main() {
	db := engine.GetNewDB()

	db.Put("MaxTemp/Novi Sad/2023-02-05", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-05", []byte{0})
	db.Put("Humidity/Novi Sad/2023-02-05", []byte{3})
	db.Put("Humidity/Belgrade/2023-02-05", []byte{4})

	db.Put("MaxTemp/Novi Sad/2023-02-06", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-06", []byte{1})
	db.Put("Humidity/Novi Sad/2023-02-06", []byte{5})
	db.Put("Humidity/Belgrade/2023-02-06", []byte{3})

	db.Put("MaxTemp/Novi Sad/2023-02-07", []byte{1})
	db.Put("MaxTemp/Belgrade/2023-02-07", []byte{2})
	db.Put("Humidity/Novi Sad/2023-02-07", []byte{5})
	db.Put("Humidity/Belgrade/2023-02-07", []byte{5})

	db.Put("MaxTemp/Novi Sad/2023-07-07", []byte{31})
	db.Put("MaxTemp/Belgrade/2023-07-07", []byte{32})
	db.Put("Humidity/Novi Sad/2023-07-07", []byte{1})
	db.Put("Humidity/Belgrade/2023-07-07", []byte{0})

	fmt.Println("Maksimalne temperature u Novom Sadu", db.List("MaxTemp/Novi Sad", 1, 2))
	fmt.Println("Maksimalne temperature u Novom Sadu str 2.", db.List("MaxTemp/Novi Sad", 2, 2))

	fmt.Println("Vlaznost vazduha u Beogradu u Februaru", db.RangeScan("Humidity/Belgrade/2023-02-01", "Humidity/Belgrade/2023-02-28", 1, 6))

	//db.CreateBloomFilter("bf", 150, 4)

}
