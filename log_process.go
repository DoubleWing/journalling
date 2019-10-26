package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/influxdata/influxdb1-client/v2"
	client "github.com/influxdata/influxdb1-client/v2"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type LogProcess struct {
	rc    chan []byte
	wc    chan *Message
	read  Reader
	write Writer
}

//读取接口
type Reader interface {
	Read(rc chan []byte)
}

//写入接口
type Writer interface {
	Write(wc chan *Message)
}

type ReadFormFile struct {
	path string //读取文件路径
}
type WriteToInfulxDB struct {
	infulxDBdsn string //infulx data source
}

type Message struct {
	TimeLocal                    time.Time
	ByteSent                     int
	Path, Method, Scheme, Status string
	UpstreamTime, RequestTime    float64
}

//用于json和结构体对象的互转
type SystemInfo struct {
	HandleLine   int     `json:"handleLine"`   //总处理日志行数
	Tps          float64 `json:"tps"`          //系统吞吐量
	ReadChanLen  int     `json:"readChanLen"`  //read channel 长度
	WriteChanLen int     `json:"writeChanLen"` //write channel 长度
	RuntTime     string  `json:"runTime"`      //运行总时间
	ErrNum       int     `josn:"errNum"`       //错误数
}

const (
	TypeHandleLine = 0
	TypeErrNum     = 1
)

var TypeMonitorChan = make(chan int, 200)

type Monitor struct {
	startTime time.Time
	data      SystemInfo
	tpsSli    []int
}

func (m *Monitor) start(lp *LogProcess) {

	go func() {
		for n := range TypeMonitorChan {
			switch n {
			case TypeErrNum:
				m.data.ErrNum += 1
			case TypeHandleLine:
				m.data.HandleLine += 1
			}
		}
	}()

	ticker := time.NewTicker(time.Second * 5)
	go func() {
		<-ticker.C
		m.tpsSli = append(m.tpsSli, m.data.HandleLine)
		if len(m.tpsSli) > 2 {
			m.tpsSli = m.tpsSli[1:]
		}
	}()

	http.HandleFunc("/monitor", func(writer http.ResponseWriter, request *http.Request) {
		m.data.RuntTime = time.Now().Sub(m.startTime).String()
		m.data.ReadChanLen = len(lp.rc)
		m.data.WriteChanLen = len(lp.wc)

		if len(m.tpsSli) >= 2 {
			m.data.Tps = float64(m.tpsSli[1]-m.tpsSli[0]) / 5
		}

		ret, _ := json.MarshalIndent(m.data, "", "\t")
		io.WriteString(writer, string(ret))
	})

	http.ListenAndServe(":9193", nil)
}

func (r *ReadFormFile) Read(rc chan []byte) {
	//读取模块
	//打开文件
	f, err := os.Open(r.path)
	if err != nil {
		panic(fmt.Sprintf("open file error %s", err.Error()))
	}

	//从文件末尾开始逐行读取文件内容
	f.Seek(0, 2)
	rd := bufio.NewReader(f)

	for {
		line, err := rd.ReadBytes('\n')
		if err == io.EOF {
			time.Sleep(500 * time.Millisecond)

			continue
		} else if err != nil {
			panic(fmt.Sprintf("ReadBytes error:%s", err.Error()))
		}
		TypeMonitorChan <- TypeHandleLine
		rc <- line[:len(line)-1]
	}
}

func (w *WriteToInfulxDB) Write(wc chan *Message) {

	infSli := strings.Split(w.infulxDBdsn, "@")

	//Create  a new HTTPClient
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     infSli[0],
		Username: infSli[1],
		Password: infSli[2],
	})
	if err != nil {
		fmt.Println("Error creating InfluxDB Client: ", err.Error())
	}

	//Create a new point batch
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  infSli[3],
		Precision: infSli[4],
	})
	if err != nil {
		fmt.Println("Error creating client.NewBatchPoints: ", err.Error())
	}

	for v := range wc {
		//Create a point and add to batch
		//Tags:Path,Method,Scheme,Status
		tags := map[string]string{"Path": v.Path, "Method": v.Method,
			"Scheme": v.Scheme, "Status": v.Status}

		//Fields: UpstreamTime , RequestTime, ByteSent
		fields := map[string]interface{}{
			"UpstreamTime": v.UpstreamTime,
			"RequestTime":  v.RequestTime,
			"ByteSent":     v.ByteSent,
		}

		pt, err := client.NewPoint("nginx_log", tags, fields, v.TimeLocal)
		if err != nil {
			fmt.Println("Error creating client.NewPoint: ", err.Error())
		}
		bp.AddPoint(pt)

		// Write the batch
		if err := c.Write(bp); err != nil {
			fmt.Println("Error creating Write the batch: ", err.Error())
		}

		log.Println("write success!")
	}

}

//文件解析模块
func (l *LogProcess) Process() {

	//正则表达式
	r := regexp.MustCompile(`([\d\,]+)\s+([^ \[]+)\s+([^ \[]+)\s+\[([^\]]+)\]\s+([a-z]+)\s+\"([^"]+)\"\s+(\d{3})\s+(\d+)\s+\"([^"]+)\"\s+\"(.*?)\"\s+\"([\d\.-]+)\"\s+([\d\.-]+)\s+([\d\.-]+)`)

	loc, _ := time.LoadLocation("Asia/Shanghai")
	for v := range l.rc {

		ret := r.FindStringSubmatch(string(v))
		if len(ret) != 14 {
			TypeMonitorChan <- TypeErrNum
			log.Println("FindStringSubmatch fail:", string(v))
			continue
		}

		message := &Message{}
		t, err := time.ParseInLocation("02/Jan/2006:15:04:05 +0000", ret[4], loc)
		if err != nil {
			TypeMonitorChan <- TypeErrNum
			log.Println("ParseInLocation fail:", err.Error(), ret[4])
		}

		message.TimeLocal = t
		byteSent, _ := strconv.Atoi(ret[8])
		message.ByteSent = byteSent

		//ByteSent
		reqSli := strings.Split(ret[6], " ")
		if len(reqSli) != 3 {
			TypeMonitorChan <- TypeErrNum
			log.Println("strings.Split fail:", ret[6])
			continue
		}
		message.Method = reqSli[0]

		//Path
		u, err := url.Parse(reqSli[1])
		if err != nil {
			TypeMonitorChan <- TypeErrNum
			log.Println("url.Parse fail:", err)
			continue
		}
		message.Path = u.Path

		//Scheme
		message.Scheme = ret[5]

		//Status
		message.Status = ret[7]

		//UpstreamTime&RequestTime
		upstreamTime, _ := strconv.ParseFloat(ret[12], 64)
		requestTime, _ := strconv.ParseFloat(ret[13], 64)
		message.UpstreamTime = upstreamTime
		message.RequestTime = requestTime

		l.wc <- message
	}

}

func main() {

	var path, influxDsn string
	flag.StringVar(&path, "Path", "access.log", "read file Path")
	flag.StringVar(&influxDsn,
		"influxDsn",
		"http://localhost:8086@zhangzhao@123456@testDB@s",
		"data source")
	flag.Parse()

	r := &ReadFormFile{
		path: path,
	}
	w := &WriteToInfulxDB{
		infulxDBdsn: influxDsn,
	}
	lp := &LogProcess{
		rc:    make(chan []byte, 200),
		wc:    make(chan *Message, 200),
		read:  r,
		write: w,
	}

	go lp.read.Read(lp.rc)
	for i := 0; i < 2; i++ {
		go lp.Process()
	}
	for i := 0; i < 4; i++ {
		go lp.write.Write(lp.wc)
	}

	m := &Monitor{
		startTime: time.Now(),
		data:      SystemInfo{},
	}

	m.start(lp)

}
