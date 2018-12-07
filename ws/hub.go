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
  "github.com/jinbanglin/bytebufferpool"
  "fmt"
)

const (
  // Time allowed to write a message to the peer.
  writeWait = 10 * time.Second

  // Time allowed to read the next pong message from the peer.
  pongWait = 60 * time.Second

  // send pings to peer with this period. Must be less than pongWait.
  pingPeriod = (pongWait * 9) / 10

  // Maximum message size allowed from peer.
  maxMessageSize = 512

  // service code size
  serviceCodeSize = 5
)

var gUpGrader = websocket.Upgrader{
  ReadBufferSize:  1024,
  WriteBufferSize: 1024,
  CheckOrigin: func(r *http.Request) bool {
    return true
  },
}

func (c *Client) setBase() {
  c.conn.SetReadLimit(maxMessageSize)
  c.conn.SetReadDeadline(time.Now().Add(pongWait))
  c.conn.SetPongHandler(func(string) error { c.conn.SetReadDeadline(time.Now().Add(pongWait)); return nil })
}

type NetPacket struct {
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
    serviceCode := helper.Byte2String(packet[0:serviceCodeSize])
    s, err := c.Hub.invoking.GetHandler(serviceCode)
    if err != nil {
      log.Error(err)
      break
    }
    req := reflect.New(s.RequestType).Interface().(proto.Message)
    if err = json.Unmarshal(packet[serviceCodeSize:], req); err != nil {
      log.Error(err)
      break
    }
    rsp, id, err := s.handler(c, req)
    if err != nil {
      log.Error(err)
      break
    }
    netPacket := bytebufferpool.Get()
    netPacket.Write(packet[0:serviceCodeSize])
    netPacket.Write(helper.Marshal2Bytes(rsp))
    broadcast(&broadcastData{id: id, userId: c.UserId, data: netPacket}, c.Hub)
  }
}

func broadcast(msg *broadcastData, hub *WsHub) {
  hub.broadcast <- msg
}

func (c *Client) writePump() {
  ticker := time.NewTicker(pingPeriod)
  defer func() {
    ticker.Stop()
    c.conn.Close()
  }()
  for {
    select {
    case packet, ok := <-c.send:
      c.conn.SetWriteDeadline(time.Now().Add(writeWait))
      if !ok {
        c.conn.WriteMessage(websocket.CloseMessage, []byte{})
        return
      }
      w, err := c.conn.NextWriter(websocket.TextMessage)
      if err != nil {
        return
      }
      w.Write(packet.Bytes())
      if err := w.Close(); err != nil {
        log.Error(err)
        return
      }
      packet.Release()
    case <-ticker.C:
      c.conn.SetWriteDeadline(time.Now().Add(writeWait))
      if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
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
      send:   make(chan *bytebufferpool.ByteBuffer),
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
  send   chan *bytebufferpool.ByteBuffer
}

type WsHub struct {
  Clients *sync.Map

  BroadcastList *sync.Map

  lock       *sync.Mutex
  maxIdl     int
  register   chan *Client
  unregister chan *Client
  broadcast  chan *broadcastData
  invoking   *Invoking
}

type broadcastData struct {
  id     string
  userId string
  data   *bytebufferpool.ByteBuffer
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
  req interface{}) (rsp interface{}, wsId string, err error)

func (h *WsHub) RegisterEndpoint(serviceCode string, req proto.Message, endpoint Endpoint) {
  h.lock.Lock()
  if len(serviceCode) != serviceCodeSize {
    panic(fmt.Sprintf("service code is must be %d,but %s\n", serviceCodeSize, serviceCode))
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

func SetupWEBSocketHub(maxIdl int) {
  GHub = &WsHub{
    lock:          new(sync.Mutex),
    Clients:       new(sync.Map),
    BroadcastList: new(sync.Map),
    register:      make(chan *Client, maxIdl),
    unregister:    make(chan *Client),
    broadcast:     make(chan *broadcastData),
    maxIdl:        maxIdl,
    invoking:      &Invoking{Scheduler: make(map[string]*SchedulerHandler)},
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
      if id, ok := h.BroadcastList.Load(packet.id); ok && len(id.([]string)) > 0 {
        for _, v := range id.([]string) {
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
