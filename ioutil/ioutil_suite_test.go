package ioutil

import (
	"log"
	"testing"

	"github.com/joho/godotenv"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pkg/errors"
)

func TestIOUtil(t *testing.T) {
	// Load environment-file.
	// Env vars will be read directly from environment if this file fails loading
	err := godotenv.Load("../.env")
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "IOUtil Suite")
}
