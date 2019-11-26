package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

const datafile = "repeating_small.json"
const workerCount = 4

func readJSON(path string) []Number {
	file, _ := ioutil.ReadFile(path)
	var data []Number
	_ = json.Unmarshal([]byte(file), &data)
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
}

// pagrindine gija
func mainThread(data []Number) {
	// deklaruoti kanalai
	send := make(chan SendParameter)
	receive := make(chan ReceiveParameter, 1000)
	results := append([]Number(nil), data...)
	resultArray := Array{Numbers: results}

	// paleidžiamos gijos
	for i := 0; i < workerCount; i++ {
		go workerThread(i, send, receive)
	}

	// pirma iteracija
	doneCount := 0
	doneLimit := len(data)
	send <- SendParameter{arr: Array{Numbers: results, Count: len(results)}, pos: 0}
	in := 0 // iteration number

	//working := true
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
	fmt.Println("rezultatai - ", results)
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
	data := readJSON(fmt.Sprintf(datafile))

	mainThread(data)

	_ = data
}
