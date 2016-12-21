package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	retryInterval = 1
)

var (
	listeningAddress    = flag.String("listeningAddress", ":8080", "Address on which to expose Prometheus metrics.")
	muninAddress        = flag.String("muninAddress", "localhost:4949", "munin-node address.")
	muninScrapeInterval = flag.Int("muninScrapeInterval", 60, "Interval in seconds between scrapes.")
	muninBanner         = regexp.MustCompile(`# munin node at (.*)`)
)

type MuninScraper struct {
	Address string

	conn     net.Conn
	hostname string
	graphs   []string

	registry         *prometheus.Registry
	gaugePerMetric   map[string]*prometheus.GaugeVec
	counterPerMetric map[string]*prometheus.CounterVec
}

func NewMuninScraper(addr string) *MuninScraper {
	return &MuninScraper{
		Address: addr,

		registry:         prometheus.NewRegistry(),
		gaugePerMetric:   map[string]*prometheus.GaugeVec{},
		counterPerMetric: map[string]*prometheus.CounterVec{},
	}
}

func (s *MuninScraper) Connect() (err error) {
	s.conn, err = net.Dial("tcp", s.Address)
	if err != nil {
		return
	}

	reader := bufio.NewReader(s.conn)
	head, err := reader.ReadString('\n')
	if err != nil {
		return
	}

	matches := muninBanner.FindStringSubmatch(head)
	if len(matches) != 2 { // expect: # munin node at <hostname>
		return fmt.Errorf("Unexpected line: %s", head)
	}
	s.hostname = matches[1]
	return
}

func (s *MuninScraper) muninCommand(cmd string) (reader *bufio.Reader, err error) {
	reader = bufio.NewReader(s.conn)

	fmt.Fprintf(s.conn, cmd+"\n")

	_, err = reader.Peek(1)
	switch err {
	case io.EOF:
		log.Printf("%s: not connected anymore, closing connection", s.Address)
		s.conn.Close()
		for {
			err = s.Connect()
			if err == nil {
				break
			}
			log.Printf("%s: Couldn't reconnect: %s", s.Address, err)
			time.Sleep(retryInterval * time.Second)
		}

		return s.muninCommand(cmd)
	case nil: //no error
		break
	default:
		log.Fatalf("%s: Unexpected error: %s", s.Address, err)
	}

	return
}

func (s *MuninScraper) muninList() (items []string, err error) {
	munin, err := s.muninCommand("list")
	if err != nil {
		log.Printf("%s: couldn't get list", s.Address)
		return
	}

	response, err := munin.ReadString('\n') // we are only interested in the first line
	if err != nil {
		log.Printf("%s: couldn't read response", s.Address)
		return
	}

	if response[0] == '#' { // # not expected here
		err = fmt.Errorf("Error getting items: %s", response)
		return
	}
	items = strings.Fields(strings.TrimRight(response, "\n"))
	return
}

func (s *MuninScraper) muninConfig(name string) (config map[string]map[string]string, graphConfig map[string]string, err error) {
	graphConfig = make(map[string]string)
	config = make(map[string]map[string]string)

	resp, err := s.muninCommand("config " + name)
	if err != nil {
		log.Printf("%s: couldn't get config for %s", s.Address, name)
		return
	}

	for {
		line, err := resp.ReadString('\n')
		if err == io.EOF {
			log.Fatalf("%s: unexpected EOF, retrying", s.Address)
			return s.muninConfig(name)
		}
		if err != nil {
			return nil, nil, err
		}
		if line == ".\n" { // munin end marker
			break
		}
		if line[0] == '#' { // here it's just a comment, so ignore it
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			return nil, nil, fmt.Errorf("Line unexpected: %s", line)
		}
		key, value := parts[0], strings.TrimRight(strings.Join(parts[1:], " "), "\n")

		keyParts := strings.Split(key, ".")
		if len(keyParts) > 1 { // it's a metric config (metric.label etc)
			if _, ok := config[keyParts[0]]; !ok { //FIXME: is there no better way?
				config[keyParts[0]] = make(map[string]string)
			}
			config[keyParts[0]][keyParts[1]] = value
		} else {
			graphConfig[keyParts[0]] = value
		}
	}
	return
}

func (s *MuninScraper) RegisterMetrics() (err error) {
	items, err := s.muninList()
	if err != nil {
		return
	}

	for _, name := range items {
		s.graphs = append(s.graphs, name)
		configs, graphConfig, err := s.muninConfig(name)
		if err != nil {
			return err
		}

		for metric, config := range configs {
			metricName := strings.Replace(name+"_"+metric, "-", "_", -1)
			desc := graphConfig["graph_title"] + ": " + config["label"]
			if config["info"] != "" {
				desc = desc + ", " + config["info"]
			}
			muninType := strings.ToLower(config["type"])
			// muninType can be empty and defaults to gauge
			if muninType == "counter" || muninType == "derive" {
				gv := prometheus.NewCounterVec(
					prometheus.CounterOpts{
						Name:        metricName,
						Help:        desc,
						ConstLabels: prometheus.Labels{"type": muninType},
					},
					[]string{"hostname", "graphname", "muninlabel"},
				)
				s.counterPerMetric[metricName] = gv
				s.registry.Register(gv)

			} else {
				gv := prometheus.NewGaugeVec(
					prometheus.GaugeOpts{
						Name:        metricName,
						Help:        desc,
						ConstLabels: prometheus.Labels{"type": "gauge"},
					},
					[]string{"hostname", "graphname", "muninlabel"},
				)
				s.gaugePerMetric[metricName] = gv
				s.registry.Register(gv)
			}
		}
	}
	return nil
}

func (s *MuninScraper) FetchMetrics() (err error) {
	for _, graph := range s.graphs {
		munin, err := s.muninCommand("fetch " + graph)
		if err != nil {
			return err
		}

		for {
			line, err := munin.ReadString('\n')
			line = strings.TrimRight(line, "\n")
			if err == io.EOF {
				log.Fatalf("%s: unexpected EOF, retrying", s.Address)
				return s.FetchMetrics()
			}
			if err != nil {
				return err
			}
			if len(line) == 1 && line[0] == '.' {
				break
			}

			parts := strings.Fields(line)
			if len(parts) != 2 {
				log.Printf("%s: unexpected line: %s", s.Address, line)
				continue
			}
			key, valueString := strings.Split(parts[0], ".")[0], parts[1]
			value, err := strconv.ParseFloat(valueString, 64)
			if err != nil {
				log.Printf("%s: Couldn't parse value in line %s, malformed?", s.Address, line)
				continue
			}
			name := strings.Replace(graph+"_"+key, "-", "_", -1)
			_, isGauge := s.gaugePerMetric[name]
			if isGauge {
				s.gaugePerMetric[name].WithLabelValues(s.hostname, graph, key).Set(value)
			} else {
				s.counterPerMetric[name].WithLabelValues(s.hostname, graph, key).Add(value)
			}
		}
	}
	return
}

func (s *MuninScraper) Handler() http.Handler {
	return promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{})
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	addrs := strings.Split(*muninAddress, ",")
	for _, addr := range addrs {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			log.Fatalf("Failed to parse: %s: %s", addr, err)
		}
		scraper := NewMuninScraper(addr)
		if err := scraper.Connect(); err != nil {
			log.Fatalf("Could not connect to %s: %s", addr, err)
		}
		http.Handle("/"+host, scraper.Handler())
		go func() {
			if err := scraper.RegisterMetrics(); err != nil {
				log.Fatalf("%s: Could not register metrics: %s", scraper.Address, err)
			}
			for {
				err := scraper.FetchMetrics()
				if err != nil {
					log.Printf("%s: Error occured when trying to fetch metrics: %s", scraper.Address, err)
				}
				time.Sleep(time.Duration(*muninScrapeInterval) * time.Second)
			}
		}()
	}

	if err := http.ListenAndServe(*listeningAddress, nil); err != nil {
		log.Fatal(err)
	}
}
