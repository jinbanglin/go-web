package ws

import (
  "sync"
  "reflect"
  "time"
  "errors"
  "net/http"
  "github.com/gogo/protobuf/proto"
  "github.com/jinbanglin/log"
  "github.com/gorilla/websocket"
  "github.com/jinbanglin/helper"
  "encoding/json"
  "fmt"
  "bytes"
  "github.com/alex023/clock"
)

const (
  // Time allowed to write a message to the peer.
  WriteWait = 10 * time.Second

  // Time allowed to read the next pong message from the peer.
  PongWait = 60 * time.Second

  // send pings to peer with this period. Must be less than PongWait.
  PingPeriod = (PongWait * 9) / 10

  // Maximum message size allowed from peer.
  MaxMessageSize = 512

  // service code size
  ServiceCodeSize = 5
)

var gUpGrader = websocket.Upgrader{
  ReadBufferSize:  1024,
  WriteBufferSize: 1024,
  CheckOrigin: func(r *http.Request) bool {
    return true
  },
}

func (c *Client) setBase() {
  c.conn.SetReadLimit(MaxMessageSize)
  c.conn.SetReadDeadline(time.Now().Add(PongWait))
  c.conn.SetPongHandler(func(string) error { c.conn.SetReadDeadline(time.Now().Add(PongWait)); return nil })
}

func (c *Client) readPump() {
  defer func() {
    c.Hub.unregister <- c
    c.conn.Close()
  }()
  c.setBase()
  for {
    t, packet, err := c.conn.ReadMessage()
    if err != nil {
      if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
        log.Error(err, t)
      }
      break
    }
    serviceCode := helper.Byte2String(packet[0:ServiceCodeSize])
    s, err := c.Hub.invoking.GetHandler(serviceCode)
    if err != nil {
      log.Error(err)
      break
    }
    req := reflect.New(s.RequestType).Interface().(proto.Message)
    if err = json.Unmarshal(packet[ServiceCodeSize:], req); err != nil {
      log.Error(err)
      break
    }
    rsp, err := s.handler(c, req)
    if err != nil {
      log.Error(err)
      break
    }
    broadcast(&broadcastData{
      id:     c.WsId,
      userId: c.UserId,
      data:   BytesCombine(packet[0:ServiceCodeSize], helper.Marshal2Bytes(rsp)),
    }, c.Hub)
  }
}

func BytesCombine(pBytes ...[]byte) []byte {
  return bytes.Join(pBytes, []byte(""))
}

func broadcast(msg *broadcastData, hub *WsHub) {
  hub.broadcast <- msg
}

func (c *Client) writePump() {
  c.Hub.clock.AddJobRepeat(PingPeriod, 0, func() {
    c.conn.SetWriteDeadline(time.Now().Add(WriteWait))
    if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
      return
    }
  })
  defer func() {
    c.conn.Close()
  }()
  for {
    select {
    case packet, ok := <-c.send:
      c.conn.SetWriteDeadline(time.Now().Add(WriteWait))
      if !ok {
        c.conn.WriteMessage(websocket.CloseMessage, []byte{})
        return
      }
      w, err := c.conn.NextWriter(websocket.TextMessage)
      if err != nil {
        return
      }
      w.Write(packet)
      if err := w.Close(); err != nil {
        log.Error(err)
        return
      }
    }
  }
}

func WSUpgrade(hub *WsHub, userId string, w http.ResponseWriter, r *http.Request) {
  client, ok := search(hub, userId)
  if !ok {
    conn, err := gUpGrader.Upgrade(w, r, nil)
    if err != nil {
      log.Error(err)
      return
    }
    client = &Client{
      UserId: userId,
      Hub:    hub,
      conn:   conn,
      send:   make(chan []byte),
    }
    client.Hub.register <- client

    go client.writePump()
    go client.readPump()
  }
}

func search(hub *WsHub, userId string) (*Client, bool) {
  v, ok := hub.Clients.Load(userId)
  if ok {
    return v.(*Client), ok
  }
  return nil, false
}

type Client struct {
  UserId string
  WsId   string
  Hub    *WsHub
  conn   *websocket.Conn
  send   chan []byte
}

type WsHub struct {
  Clients *sync.Map

  BroadcastList *sync.Map

  broadcastWay int
  lock         *sync.Mutex
  maxIdl       int
  register     chan *Client
  unregister   chan *Client
  broadcast    chan *broadcastData
  invoking     *Invoking

  clock *clock.Clock
}

type broadcastData struct {
  id     string
  userId string
  data   []byte
}

type Invoking struct {
  Scheduler map[string]*SchedulerHandler
}

type SchedulerHandler struct {
  RequestType reflect.Type
  handler     Endpoint
}

type Endpoint func(
  client *Client,
  req interface{}) (rsp interface{}, err error)

func (h *WsHub) RegisterEndpoint(serviceCode string, req proto.Message, endpoint Endpoint) {
  h.lock.Lock()
  if len(serviceCode) != ServiceCodeSize {
    panic(fmt.Sprintf("service code is must be %d,but %s\n", ServiceCodeSize, serviceCode))
  }
  if _, ok := h.invoking.Scheduler[serviceCode]; ok {
    panic("handler is already register")
  }
  h.invoking.Scheduler[serviceCode] = &SchedulerHandler{
    RequestType: reflect.TypeOf(req).Elem(),
    handler:     endpoint,
  }
  h.lock.Unlock()
}

func (i *Invoking) GetHandler(serviceCode string) (handler *SchedulerHandler, err error) {
  var ok bool
  if handler, ok = i.Scheduler[serviceCode]; !ok {
    log.Error(" |no service code=", serviceCode)
    return nil, errors.New("no service")
  }
  return
}

var GHub *WsHub

func SetupWEBSocketHub(maxIdl, broadcastWay int) {
  GHub = &WsHub{
    broadcastWay:  broadcastWay,
    lock:          new(sync.Mutex),
    Clients:       new(sync.Map),
    BroadcastList: new(sync.Map),
    register:      make(chan *Client, maxIdl),
    unregister:    make(chan *Client),
    broadcast:     make(chan *broadcastData),
    maxIdl:        maxIdl,
    invoking:      &Invoking{Scheduler: make(map[string]*SchedulerHandler)},
    clock:         clock.NewClock(),
  }
  go GHub.Run()
}

func (h *WsHub) Run() {
  h.lock.Lock()
  for {
    select {
    case client := <-h.register:
      h.Clients.Store(client.UserId, client)
    case client := <-h.unregister:
      if _, ok := h.Clients.Load(client.UserId); ok {
        close(client.send)
        h.Clients.Delete(client.UserId)
      }
    case packet := <-h.broadcast:
      var result []string
      if h.broadcastWay == 1 {
        b, err := helper.GRedisRing.Get(packet.id).Bytes()
        if err == nil {
          json.Unmarshal(b, &result)
        }
      } else {
        if id, ok := h.BroadcastList.Load(packet.id); ok {
          result = id.([]string)
        }
      }
      if len(result) > 0 {
        for _, v := range result {
          if cnn, ok := h.Clients.Load(v); ok {
            cnn.(*Client).send <- packet.data
          }
        }
      } else {
        if cnn, ok := h.Clients.Load(packet.userId); ok {
          cnn.(*Client).send <- packet.data
        }
      }
    }
  }
  h.lock.Unlock()
}
