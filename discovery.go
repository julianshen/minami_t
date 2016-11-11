package minami_t

import (
	"fmt"
	etcd "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"log"
	"regexp"
	"time"
)

type ServiceRegistry struct {
	etcd_client etcd.KeysAPI
	name        string
	ctx         context.Context
}

type Node struct {
	Name string
	Url  string
}

type Watcher func(list []Node)

func NewServiceRegistry(name string, endpoints []string) (*ServiceRegistry, error) {
	cfg := etcd.Config{
		Endpoints: endpoints,
		Transport: etcd.DefaultTransport,
		// set timeout per request to fail fast when the target endpoint is unavailable
		HeaderTimeoutPerRequest: time.Second,
	}

	c, err := etcd.New(cfg)
	if err != nil {
		return nil, err
	}

	kapi := etcd.NewKeysAPI(c)
	sr := ServiceRegistry{kapi, name, context.TODO()}
	return &sr, nil
}

func (s *ServiceRegistry) Register(name string, _url string) error {
	//key name
	key := fmt.Sprintf("%s/nodes/%s", s.name, name)

	_, err := s.etcd_client.Set(s.ctx, key, _url, nil)

	return err
}

func (s *ServiceRegistry) Unregister(name string) error {
	//key name
	key := fmt.Sprintf("%s/nodes/%s", s.name, name)
	log.Printf("Unregister %s\n", key)

	_, err := s.etcd_client.Delete(s.ctx, key, &etcd.DeleteOptions{Recursive: true})

	return err
}

func (s *ServiceRegistry) GetNodes() ([]Node, error) {
	key := fmt.Sprintf("/%s/nodes", s.name)
	getResp, err := s.etcd_client.Get(s.ctx, key, &etcd.GetOptions{Recursive: true})

	if err != nil {
		return nil, err
	}

	var list []Node
	s1 := fmt.Sprintf("\\/%s\\/nodes\\/(.*)", s.name)
	regex := regexp.MustCompile(s1)

	if len(getResp.Node.Nodes) == 0 && getResp.Node.Value != "" {
		list = make([]Node, 1)
		var name string
		result := regex.FindAllStringSubmatch(getResp.Node.Key, -1)

		if len(result) > 0 && len(result[0]) > 1 {
			name = result[0][1]
		} else {
			name = getResp.Node.Key
		}

		list[0].Name = name
		list[0].Url = getResp.Node.Value
	} else if len(getResp.Node.Nodes) != 0 {
		list = make([]Node, len(getResp.Node.Nodes))

		for i, n := range getResp.Node.Nodes {
			var name string
			result := regex.FindAllStringSubmatch(n.Key, -1)
			if len(result) > 0 && len(result[0]) > 1 {
				name = result[0][1]
			} else {
				name = getResp.Node.Key
			}
			list[i] = Node{name, n.Value}
		}
	} else {
		list = make([]Node, 0)
	}

	return list, err
}

func (s *ServiceRegistry) Watch(watcher Watcher) {
	key := fmt.Sprintf("/%s/nodes", s.name)
	log.Println("watch " + key)
	w := s.etcd_client.Watcher(key, &etcd.WatcherOptions{AfterIndex: 0, Recursive: true})

	var retryInterval time.Duration = 1

	for {
		_, err := w.Next(s.ctx)

		if err != nil {
			log.Printf("Failed to connect to etcd. Will retry after %d sec \n", retryInterval)
			time.Sleep(retryInterval * time.Second)

			retryInterval = (retryInterval * 2) % 4096
		} else {
			if retryInterval > 1 {
				retryInterval = 1
			}

			list, err := s.GetNodes()
			if err == nil {
				watcher(list)
			} else {
				//skip first
			}
		}
	}
}
