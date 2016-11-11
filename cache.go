package minami_t

import (
	"log"

	"crypto/sha1"
	"errors"
	"fmt"
	"github.com/golang/groupcache"
	"github.com/julianshen/vips"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
)

var (
	ErrWrongKey        = errors.New("Wrong key format")
	ErrWrongSizeParams = errors.New("Size parameter is wrong")
)

type CacheManager struct {
	cache      *groupcache.Group
	pool       *groupcache.HTTPPool
	url        string
	reg        *ServiceRegistry
	downloader *Downloader
}

func NewCache(name string, size int64, port int) (*CacheManager, error) {
	ip, err := getMyIp()

	if err != nil {
		//Does not have a network interface?
		return nil, err
	}

	if port < 0 {
		port = 0
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, err
	}

	if port == 0 {
		port = ln.Addr().(*net.TCPAddr).Port
	}

	_url := fmt.Sprintf("http://%s:%d", ip, port)
	pool := groupcache.NewHTTPPool(_url)

	go func() {
		log.Printf("Group cache served at port %s:%d\n", ip, port)
		if err := http.Serve(ln, http.HandlerFunc(pool.ServeHTTP)); err != nil {
			log.Printf("GROUPCACHE PORT %d, ERROR: %s\n", port, err.Error())
			os.Exit(-1)
		}
	}()

	cache := groupcache.NewGroup(name, size, groupcache.GetterFunc(getter))
	downloader := NewDownloader("cache_" + name + "/")

	return &CacheManager{cache, pool, _url, nil, &downloader}, nil
}

func (cm *CacheManager) Join(sr *ServiceRegistry) error {
	//Use hashed url as ID to prevent duplication
	name := hashId(cm.url)

	log.Printf("Register this node as %s\n", name)
	sr.Register(name, cm.url)

	//Init peers
	nodes, err := sr.GetNodes()

	if err != nil {
		return err
	}

	addPeers(cm.pool, nodes)

	go sr.Watch(func(nodes []Node) {
		addPeers(cm.pool, nodes)
	})

	cm.reg = sr

	return nil
}

func (cm *CacheManager) Leave() {
	if cm.reg != nil {
		name := hashId(cm.url)
		err := cm.reg.Unregister(name)

		if err != nil {
			log.Fatal(err)
		}
	}
}

func (cm *CacheManager) Get(key string) ([]byte, error) {
	var data []byte
	err := cm.cache.Get(cm.downloader, key, groupcache.AllocatingByteSliceSink(&data))
	return data, err
}

func getMyIp() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", errors.New("No available network interface")
}

func addPeers(pool *groupcache.HTTPPool, nodes []Node) {
	if nodes == nil || len(nodes) == 0 {
		return
	}

	peers := make([]string, len(nodes))

	for i, n := range nodes {
		log.Println("add " + n.Url)
		peers[i] = n.Url
	}

	pool.Set(peers...)
}

//Key format - width:height:fileName
func getter(ctx groupcache.Context, key string, dest groupcache.Sink) error {
	log.Println("Cache missed for " + key)

	params := strings.Split(key, ":")

	if len(params) != 3 {
		return ErrWrongKey
	}

	d := ctx.(*Downloader)
	fileName, err := d.Download("http://i.imgur.com/" + params[2])

	if err != nil {
		return err
	}

	//Should assume correct since it is checked at where it is from
	width, _ := strconv.Atoi(params[0])
	height, _ := strconv.Atoi(params[1])

	data, err := resize(fileName, width, height)

	if err != nil {
		return err
	}

	dest.SetBytes(data)
	return nil
}

func hashId(s string) string {
	return fmt.Sprintf("%02x", sha1.Sum([]byte(s)))
}

//Resize with VIPS
func resize(fileName string, width int, height int) ([]byte, error) {
	options := vips.Options{
		Width:        width,
		Height:       height,
		Crop:         true,
		Extend:       vips.EXTEND_WHITE,
		Enlarge:      true,
		Interpolator: vips.BICUBIC,
		Gravity:      vips.CENTRE,
		Quality:      95,
		Savetype:     vips.JPEG,
	}

	f, _ := os.Open(fileName)
	defer f.Close()
	inBuf, _ := ioutil.ReadAll(f)
	buf, err := vips.Resize(inBuf, options)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return buf, nil
}
