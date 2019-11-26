package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"time"
)

const datafile = "file.json"
const workerCount = 2
const valuesCount = 10

func readJSON(path string) []Number {
	file, _ := ioutil.ReadFile(path)
	var data []Number
	err := json.Unmarshal([]byte(file), &data)
	if err != nil {
		fmt.Println("error:", err)
	}
	return data
}

// Number is number
type Number struct {
	Number int
}

// Array is array that stores Numbers and their count
type Array struct {
	Numbers []Number
	Count   int
}

func (arr *Array) add(value int) {
	(*arr).Numbers[(*arr).Count].Number = value
	(*arr).Count++
}

//pertvarko masyva kad pries pivot butu mazesniu reiksmiu masyvas, po - didesniu
func (arr *Array) reorganise(iterationResults ReceiveParameter) {
	start := iterationResults.pos
	end := start + iterationResults.Low.Count
	j := 0
	for i := start; i < end; i++ {
		j++
		arr.Numbers[i].Number = iterationResults.Low.Numbers[j].Number
	}
	start = end
	end += iterationResults.count
	for i := start; i < end; i++ {
		arr.Numbers[i].Number = iterationResults.pivot
	}
	start = end
	end += iterationResults.High.Count
	j = 0
	for i := start; i < end; i++ {
		j++
		arr.Numbers[i].Number = iterationResults.High.Numbers[j].Number
	}
}

//naudoti vietoj reorganise metodo, jeigu norima nesaugoti tarpiniu skaiciavimu
//vietoj to, kad neapskaičiuotos reikšmės būtų rašomos būtų užrašomos masyve, jos yra laikomos kanaluose arba darbinese gijose
func (arr *Array) insert(iterationResults ReceiveParameter) {
	start := iterationResults.pos
	end := start + iterationResults.Low.Count
	start = end
	end += iterationResults.count
	for i := start; i < end; i++ {
		arr.Numbers[i].Number = iterationResults.pivot
	}
}

// SendParameter is value that is sent from main thread to worker thread via channel
type SendParameter struct {
	arr Array
	pos int
}

// ReceiveParameter is value that is sent from main thread to worker thread via channel
type ReceiveParameter struct {
	Low   Array // mažesnių skaičių už pivot masyvas
	High  Array // didesnių skaičių už pivot masyvas
	count int   // pivot kiekis
	pivot int   // pivot - element to be placed at right position
	pos   int   // pozicija, į kurią turetų būti rašoma pi reikšmė
	in    int
}

// pagrindine gija
func mainThread(data []Number, threadCount int) {
	// deklaruoti kanalai
	send := make(chan SendParameter, len(data))
	receive := make(chan ReceiveParameter)
	results := append([]Number(nil), data...)
	resultArray := Array{Numbers: results}

	// paleidžiamos gijos
	for i := 0; i < threadCount; i++ {
		go workerThread(i, send, receive)
	}

	// pirma iteracija
	doneCount := 0
	doneLimit := len(data)
	send <- SendParameter{arr: Array{Numbers: results, Count: len(results)}, pos: 0}
	in := 0 // iteration number

	for doneLimit != doneCount {
		// fmt.Println("iteration - " in)
		// fmt.Println(results)
		// fmt.Println("--------------------")

		in++
		temp := <-receive

		if temp.Low.Count > 0 {
			send <- SendParameter{arr: temp.Low, pos: temp.pos}
		}
		if temp.High.Count > 0 {
			send <- SendParameter{arr: temp.High, pos: temp.pos + temp.Low.Count + temp.count}
		}
		doneCount += temp.count
		resultArray.reorganise(temp)
		//resultArray.insert(temp)
	}
	//fmt.Println("rezultatai - ", results)
	close(send)
}

/* Worker thread - partitioning
Gija darbininke padalina masyva i dvi dalis
Pagal paskutinę reikšmę padaro 2 mažesnius masyvus
Gražina masyvus, paskutinio skaičiaus reikšmę, ir kiek kartų tas skaičius pasikartojo
*/
func workerThread(id int, receive chan SendParameter, send chan ReceiveParameter) {
	for i := range receive {
		// paskutine reikšmė
		pivot := i.arr.Numbers[i.arr.Count-1].Number
		// reikšmės pasikartojimų kiekis
		count := 1
		lowArr := make([]Number, i.arr.Count)
		highArr := make([]Number, i.arr.Count)
		// mažesnių už pivot masyvas
		low := Array{Numbers: lowArr, Count: 0}
		// didesnių už pivot masyvas
		high := Array{Numbers: highArr, Count: 0}

		// lygina visus skaičius su pivot
		for j := 0; j < i.arr.Count-1; j++ {
			switch {
			case pivot == i.arr.Numbers[j].Number:
				count++
			case pivot > i.arr.Numbers[j].Number:
				low.add(i.arr.Numbers[j].Number)
			case pivot < i.arr.Numbers[j].Number:
				high.add(i.arr.Numbers[j].Number)
			}
		}

		// siunčia pranešimą į kanalą, iš kurio skaito pagrindine gija
		send <- ReceiveParameter{Low: low, High: high, count: count, pivot: pivot, pos: i.pos}
	}
}

func main() {
	// createRandomJSON(valuesCount)
	// data := readJSON(fmt.Sprintf(datafile))
	// start := time.Now()
	// mainThread(data, workerCount)
	// fmt.Println(time.Since(start))

	speedtest(10, 32, []int{100, 1000, 10000})

	//createRandomJSON(100, 0)

}

func speedtest(times int, threadLimit int, dataSets []int) {
	f, _ := os.Create("./results.csv")
	w := bufio.NewWriter(f)
	for i := 0; i < len(dataSets); i++ {
		createRandomJSON(dataSets[i], i)
		w.WriteString(",")
		w.WriteString(fmt.Sprintf("%d", dataSets[i]))
	}
	w.WriteString("\n")
	for j := 1; j <= threadLimit; j *= 2 {
		w.WriteString(fmt.Sprintf("%d gija,", j))
		fmt.Println("================ giju skaicius - ", j)
		for k := 0; k < len(dataSets); k++ {
			data := readJSON(fmt.Sprintf("./gen/file%d.json", k))
			start := time.Now()
			for i := 0; i < times; i++ {
				copy := append([]Number(nil), data...)
				mainThread(copy, j)
			}
			time := time.Since(start).Nanoseconds()
			avgTime := time / int64(times)
			fmt.Print("duomenu skaicius -", dataSets[k], ">>>>>>")
			fmt.Println("laikas - ", time, "padalintas laikas - ", avgTime)
			w.WriteString(fmt.Sprintf("%d,", avgTime))
		}
		w.WriteString("\n")
	}
	w.Flush()
}

func createRandomJSON(count int, i int) {
	f, _ := os.Create(fmt.Sprintf("./gen/file%d.json", i))
	w := bufio.NewWriter(f)
	w.WriteString("[\n")
	for i := 0; i < count; i++ {
		val := (rand.Intn(count))
		mapD := map[string]int{"Number": val}
		mapB, _ := json.Marshal(mapD)
		w.WriteString(string(mapB))
		if i < count-1 {
			w.WriteString(",")
		}
		w.WriteString("\n")
	}
	w.WriteString("\n]")
	w.Flush()
}
