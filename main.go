package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"time"
  "strconv"
  "strings"
)

type Config struct {
  Port int 
  SendAcks bool
  VerboseLogging bool
  Fields []FieldValidator 
}

type FieldValidator struct {
  LineName string
  FieldNumber int
  NonNullable bool
  Type string
  Pattern string
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

type FieldResult struct {
  LineName string
  FieldNumber string
  Result string
}

type FailOutCome struct {
  Expected []FieldResult 
  Found []FieldResult 
  OutcomeSummary []string
  RegexFail bool
  NullFail bool
  TypeFail bool
}

func (f *FailOutCome) DidAllConfigsPass() bool {
  if (f.RegexFail || f.NullFail || f.TypeFail){
    return false
  }
  return true
}

func CheckAgainstConfigs (config *Config, hl7 *HL7) FailOutCome {
  outcome := FailOutCome{}
  for _, check := range config.Fields {
    for _, line := range hl7.Segments {
      if(check.LineName[:3] == line.Header){
        //Check if were doing more detailed line
        if(len(check.LineName) > 3){
          if(check.LineName[4:] != line.Fields[1]){
            break
          } 
        }
        outcomeSummary := "Pass";

        var spot string

        //NOTE Special handling for MSH because the split grabs something different than 7edit ui 
        if(check.LineName == "MSH"){
          spot = line.Fields[check.FieldNumber-1]
        } else {
          spot = line.Fields[check.FieldNumber]
        }
        //nullable
        if (check.NonNullable && spot == ""){
          outcome.NullFail = true
          outcomeSummary = "Found an empty field in that position."
        }
        //type
        if(check.Type == "number") {
          _, err := strconv.Atoi(spot)
          if err != nil {
            outcome.TypeFail = true
            outcomeSummary = "Unable to convert that field to a Number."
          }
        }
        //Pattern
        if(check.Pattern != ""){
          if(!strings.Contains(spot, check.Pattern)){
            match, _ := regexp.MatchString(check.Pattern,spot)
            if(!match){
              outcome.RegexFail = true
              outcomeSummary = "Unable to find a match using the reggex pattern passed in."
            }
          }
        }

        expected := FieldResult{
          LineName: check.LineName,
          FieldNumber: strconv.Itoa(check.FieldNumber),
          Result: check.Pattern,
        }

        found := FieldResult{
          LineName: line.Header,
          FieldNumber: strconv.Itoa(check.FieldNumber),
          Result: spot,
        }

        outcome.Expected = append(outcome.Expected, expected)
        outcome.Found = append(outcome.Found, found)
        outcome.OutcomeSummary = append(outcome.OutcomeSummary, outcomeSummary)
      }
    }

  }
  return outcome
}

func PrintSuccess(message string){
      // ANSI escape code for green color
    green := "\033[32m"

    // ANSI escape code to reset color
    reset := "\033[0m"

    fmt.Printf("%s%s%s\n", green, message, reset)
}

func PrintFailure(message string){
  // ANSI escape code for red color
    red := "\033[31m"

    // ANSI escape code to reset color
    reset := "\033[0m"

    fmt.Printf("%s%s%s\n", red, message, reset)
}

func (o *FailOutCome) Print (verbose bool, senderInfo string){
  success := o.DidAllConfigsPass()
  if(success) {
    PrintSuccess(fmt.Sprintf("%c Message from %s validated successfully. \n", '\u2714', senderInfo))
  } else {
    PrintFailure(fmt.Sprintf("%c Message from %s failed validation. \n", '\u2718', senderInfo))
  }
  
  if(verbose){
    fmt.Printf("==========VERBOSE LOGGING for message from %s========== \n",senderInfo)
    for i,msg := range o.OutcomeSummary {
      fmt.Println(">>>>>")
      fmt.Printf("Summary: %s \n",msg)
      fmt.Println("----------")
      fmt.Println("Expected:")
      fmt.Printf("Looking at %s %s \n", o.Expected[i].LineName, o.Expected[i].FieldNumber)
      fmt.Printf("Trying to find or match this pattern: %s \n", o.Expected[i].Result)
      fmt.Println("----------")
      fmt.Println("FOUND:")
      fmt.Printf("Looking at %s %s \n", o.Found[i].LineName, o.Found[i].FieldNumber)
      fmt.Printf("Found this in the field listed above %s \n", o.Found[i].Result)
      fmt.Println("<<<<<")
    }
    fmt.Printf("========== End VERBOSE LOGGING for message from %s========== \n",senderInfo)
  }
}


func main() {
  //Load configurations
  config, err := LoadConfigs()
  if err != nil {
    fmt.Printf("Error loading configs: %s\n", err)
    return
  }

  fmt.Println("Configs loaded!")
  
  //Open Listener
  server := NewServer(":" + strconv.Itoa(config.Port),config.SendAcks)


  //Validate messages based on configurations
  go func(){
    fmt.Println("Reading from the channel")
    for msg := range server.msgChan{
      result := CheckAgainstConfigs(config, msg.Message)
      result.Print(config.VerboseLogging, msg.Sender.RemoteAddr().String())
    }
  }()

  log.Fatal(server.Start())

  fmt.Printf("HL7 Validator started and listening on port %s\n", strconv.Itoa(config.Port))
} 
