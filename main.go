package main

import (
  "fmt"
  "encoding/json"
  "os"
  "net"
  "log"
  "strconv"
  "bytes"
  "strings"
  "time"
  "io"
)

type Config struct {
  Port int 
  SendAcks bool
  Fields map[string]string
}
func LoadConfigs() (*Config, error){
  data, err := os.ReadFile("config.json")
  if err != nil {
    return nil, err
  }

  var config Config
  err = json.Unmarshal(data, &config)
  if err != nil {
    return nil, err
  }

  return &config, nil
}

type HL7 struct {
  Version string
  ControlId string
  Segments []Segment
  ValidMSH bool
}

type Segment struct {
  Header string
  NumFields int
  Fields []string
}

func newHl7Message (rawMessage []byte) *HL7{
  hl7 := &HL7{}

  segments := bytes.Split(rawMessage,[]byte("\r"))
  for _, seg := range segments {
    fields := strings.Split(string(seg), "|")
    if len(fields) > 0 {
      hl7.Segments = append(hl7.Segments, Segment{
        Header: fields[0],
        NumFields: len(fields),
        Fields: fields,
      })
    }
  }
  return hl7
}

func (m *HL7) validateMSH () {
  if(len(m.Segments) == 0 || m.Segments[0].Header != "MSH"){
    m.ValidMSH = false
    return
  }

  var firstLine = m.Segments[0]

  if (firstLine.Fields[9] == "" || firstLine.Fields[11] == "") {
    m.ValidMSH = false;
    return
  }

  m.ControlId = firstLine.Fields[9]
  m.Version = firstLine.Fields[11]
  m.ValidMSH = true;
}

type Message struct {
  Sender net.Conn
  Message  *HL7
}

type Server struct {
  listenPort string
  sendAcks bool
  listener net.Listener
  quitChan chan struct{}
  msgChan chan Message 
}

func NewServer(port string, ack bool) *Server {
  return &Server{
    listenPort: port,
    sendAcks: ack,
    quitChan: make(chan struct{}),
    msgChan: make(chan Message, 10),
  }
}

func (s *Server) Start () error {
  listener, err := net.Listen("tcp", s.listenPort)
  if err != nil {
    return err
  }
  defer listener.Close()
  s.listener = listener

  fmt.Printf("HL7 Validator started and listening on port %s\n", s.listenPort)

  go s.acceptLoop()

  <-s.quitChan
  close(s.msgChan)

  return nil
}

func (s *Server) acceptLoop() {
  for {
    conn, err := s.listener.Accept()
    if err != nil {
      fmt.Printf("Error accepting the connection %s \n", err)
      continue
    }
    fmt.Printf("New Connection made. %s\n", conn.RemoteAddr())
    go s.readLoop(conn)
  }
}

func (s *Server) readLoop(conn net.Conn) {
  defer conn.Close()
  buf := make([]byte, 2048)
  var rawMsg []byte
  startOfMessage := byte(0x0B)
  endOfMessage   := []byte{0x1c,0x0D}

  for {
    n, err := conn.Read(buf)
    if err != nil {
      if err == io.EOF {
        fmt.Println("Connection closed by sender")
      } else {
      fmt.Printf("Error reading from connection to buffer %s", err)
      }
      break 
    }
    
    rawMsg = append(rawMsg, buf[:n]...)
    startIdx := bytes.Index(rawMsg, []byte{startOfMessage})
    endIdx   := bytes.Index(rawMsg, endOfMessage)

    if startIdx != -1  && endIdx != -1 {
      //Got a full HL7 Message
      hl7 := newHl7Message(rawMsg[startIdx+len([]byte{startOfMessage}):endIdx])
      hl7.validateMSH()

      if (hl7.ValidMSH){
        s.msgChan <- Message {
          Sender: conn,
          Message: hl7,
        }
        //Ack feature here? 
        if(s.sendAcks){
          fmt.Printf("Ack sent to %s\n", conn.RemoteAddr())
          ackMsg := fmt.Sprintf("\x0BMSH|^~\\&|||goValidateHL7|goValidateHL7|%s||ACK||D|%s\x0DMSA|AA|%s\x1C\x0D", time.Now().Format("20060102150405"), hl7.Version, hl7.ControlId)
          conn.Write([]byte(ackMsg))
        }
      } else {
        fmt.Printf("Received invalid HL7 Message from %s \n", conn.RemoteAddr())
      }
      rawMsg = rawMsg[endIdx+len(endOfMessage):]
    }
  }
}


func main() {
  //Load configurations
  config, err := LoadConfigs()
  if err != nil {
    fmt.Printf("Error loading configs: %s\n", err)
    return
  }

  fmt.Printf("Configs loaded!\n")
  
  //Open Listener
  server := NewServer(":" + strconv.Itoa(config.Port),config.SendAcks)


  //Validate messages based on configurations
  go func(){
    fmt.Print("Reading from the channel")
    for msg := range server.msgChan{
      fmt.Printf("Received message %s \n ", msg.Message.Segments[0].Fields[0])
    }
  }()

  log.Fatal(server.Start())

  fmt.Printf("HL7 Validator started and listening on port %s\n", strconv.Itoa(config.Port))
} 
