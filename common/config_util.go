package common

import (
	"github.com/go-playground/validator/v10"
	"github.com/spf13/viper"
)

func ReadConfigFromFile() (*Config, error) {
	viper.SetConfigFile("config.yml")
	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}

	// Unmarshal config into struct
	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, err
	}

	// Validate the configuration using validator
	validate := validator.New()
	validate.RegisterValidation("clustermode", clusterModeValidator)

	if err := validate.Struct(config); err != nil {
		return nil, err
	}

	return &config, nil
}
