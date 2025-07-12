package configo

import (
	"SMS/internal/config"

	"github.com/ilyakaznacheev/cleanenv"
	"os"
)

func MustLoad(TConfig config.Config) *config.Config {
	path := "config/local.yaml"

	if _, err := os.Stat(path); err != nil {
		panic("Файл конфига не найден")
	}

	if err := cleanenv.ReadConfig(path, &TConfig); err != nil {
		panic("Ошибка загрузки конфига: " + err.Error())
	}

	return &TConfig
}
