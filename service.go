package minami_t

import (
	"log"
	"net/http"
	"strconv"

	"fmt"
	"github.com/gin-gonic/gin"
	"net"
	"os"
	"os/signal"
	"syscall"
	"strings"
)

const (
	MAX_WIDTH  = 1920
	MAX_HEIGHT = 1920
)

var (
	cm *CacheManager
	cache_name string
	cache_size int64
	etcd_endpoints []string
	cache_port int
	service_port int
)

const (
	DEFAULT_CACHE_NAME = "mycache"
	DEFAULT_LOCAL_CACHE_SIZE = 64<<20
	DEFAULT_ETCD_ENDPOINTS = "http://127.0.0.1:2379"
	DEFAULT_CACHE_PORT = 0
	DEFAULT_SERVICE_PORT = 0
)

func getEnv(key string, defValue string) string {
	val := os.Getenv(key)
	if val == "" {
		val = defValue
	}

	return val
}

func getEnvInt(key string, defValue int64) int64 {
	val := os.Getenv(key)
	if val == "" {
		return defValue
	}

	n, err := strconv.ParseInt(val, 0, 64)
	if err != nil {
		return defValue
	}
	return n
}

func initFromEnvs() {
	cache_name = getEnv("CACHE_NAME", DEFAULT_CACHE_NAME)
	cache_size = getEnvInt("CACHE_SIZE", DEFAULT_LOCAL_CACHE_SIZE)
	endpoints := getEnv("ETCD_ENDPOINTS", DEFAULT_ETCD_ENDPOINTS)
	etcd_endpoints = strings.Split(endpoints, ";")
	cache_port = int(getEnvInt("CACHE_PORT", DEFAULT_CACHE_PORT))
	service_port = int(getEnvInt("SERVICE_PORT", DEFAULT_SERVICE_PORT))

	log.Printf("Cache name : %s\n", cache_name)
	log.Printf("Cache size : %d\n", cache_size)
	log.Printf("Etcd endpoints : %s\n", endpoints)
	log.Printf("Cache port : %d\n", cache_port)
	log.Printf("Service port : %d\n", service_port)
}

func init() {
	log.Println("Init")
	initFromEnvs()

	var err error
	cm, err = NewCache(cache_name, cache_size, cache_port)
	if err != nil {
		panic("error to init cache")
	}

	sr, err := NewServiceRegistry(cache_name, etcd_endpoints)
	if err == nil {
		cm.Join(sr)
	} else {
		log.Println("There is no service registry (etcd) found. Starts with single node")
	}

	//Listening to exit signals for graceful leave
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
		<-c
		log.Println("I'm leaving")
		cm.Leave()
		os.Exit(0)
	}()
}

//Not safe. Might race with other process
func getFreePort() int {
	ln, err := net.Listen("tcp", ":0")
	if err != nil {
		panic("error")
	}
	defer ln.Close()

	return ln.Addr().(*net.TCPAddr).Port
}

func Serve() {
	router := gin.Default()
	router.GET("/t/:width/:height/:fileName", func(ctx *gin.Context) {
		fileName := ctx.Param("fileName")
		s_width := ctx.Param("width")
		s_height := ctx.Param("height")

		var width int
		var height int
		var err error

		if width, err = strconv.Atoi(s_width); err != nil || width <= 0 || width > MAX_WIDTH {
			ctx.String(http.StatusBadRequest, "Invalid width")
			return
		}

		if height, err = strconv.Atoi(s_height); err != nil || height <= 0 || height > MAX_HEIGHT {
			ctx.String(http.StatusBadRequest, "Invalid height")
			return
		}

		key := fmt.Sprintf("%d:%d:%s", width, height, fileName)
		result, err := cm.Get(key)

		if err != nil {
			ctx.String(http.StatusBadRequest, err.Error())
			return
		}

		ctx.Data(http.StatusOK, "image/jpeg", result)
	})

	if service_port == 0 {
		service_port = getFreePort()
	}
	router.Run(fmt.Sprintf(":%d", service_port))
}
