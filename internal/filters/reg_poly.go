package filters

import (
	"fmt"
	"gonum.org/v1/gonum/mat"
)

// getSmoothedValues возвращает сглаженные значения, вычисленные по квадратичной регрессии
func GetSmoothedValues(t, y []float64) ([]float64, bool) {
	n := len(t)
	if n != len(y) {
		fmt.Println("Ошибка: длины массивов t и y должны совпадать")
		return nil, false
	}

	if n < 3 {
		fmt.Printf("Ошибка: недостаточно данных для полинома 2-й степени. Нужно минимум 3 точки, получено %d\n", n)
		return nil, false
	}

	// Создаем матрицу X: [1, t, t^2]
	X := mat.NewDense(n, 3, nil)
	for i := 0; i < n; i++ {
		X.Set(i, 0, 1)         // свободный член
		X.Set(i, 1, t[i])      // линейный член
		X.Set(i, 2, t[i]*t[i]) // квадратичный член
	}

	// Преобразуем y в матрицу-столбец
	yVec := mat.NewDense(n, 1, y)

	// Вычисляем beta = (X^T * X)^(-1) * X^T * y
	// Сначала вычисляем транспонированную матрицу
	rows, cols := X.Dims()
	Xtrans := *mat.NewDense(cols, rows, nil) // Создаем матрицу размером cols × rows

	for i := 0; i < rows; i++ {
		for j := 0; j < cols; j++ {
			Xtrans.Set(j, i, X.At(i, j)) // Транспонируем вручную
		}
	}

	// Теперь делаем Xt = X^T * X
	var Xt mat.Dense
	Xt.Mul(&Xtrans, X)

	var invXt mat.Dense
	if err := invXt.Inverse(&Xt); err != nil {
		fmt.Println("Ошибка: не удалось обратить матрицу")
		return nil, false
	}

	// beta = invXt * Xtrans → размер 3 × n
	var betaTemp mat.Dense
	betaTemp.Mul(&invXt, &Xtrans)

	// betaFinal = betaTemp * yVec → размер 3 × 1
	var betaFinal mat.Dense
	betaFinal.Mul(&betaTemp, yVec)

	// Вычисляем сглаженные значения
	smoothed := make([]float64, n)
	for i := 0; i < n; i++ {
		a := betaFinal.At(0, 0)
		b := betaFinal.At(1, 0)
		c := betaFinal.At(2, 0)
		smoothed[i] = a + b*t[i] + c*t[i]*t[i]
	}

	return smoothed, true
}
