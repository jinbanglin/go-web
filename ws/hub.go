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
  "github.com/json-iterator/go"
  "github.com/jinbanglin/micro/message"
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
)

var gUpGrader = websocket.Upgrader{
  ReadBufferSize:  1024,
  WriteBufferSize: 1024,
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
    _, packet, err := c.conn.ReadMessage()
    if err != nil {
      if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
        log.Error(err, )
      }
      break
    }
    userLoad := &msg.WsPacket{}
    if err = jsoniter.Unmarshal(packet, userLoad); err != nil {
      log.Error(err)
      break
    } else {
      s, err := c.Hub.invoking.GetHandler(userLoad.ServiceCode)
      if err != nil {
        log.Error(err)
        break
      }
      req := reflect.New(s.RequestType).Interface().(proto.Message)
      if err = jsoniter.Unmarshal(userLoad.Payload, req); err != nil {
        log.Error(err)
        break
      }
      rsp, id, err := s.handler(c, req)
      if err != nil {
        log.Error(err)
        break
      }
      broadcast(&msg.WsPacket{
        ServiceCode: userLoad.ServiceCode,
        Payload:     helper.Marshal2Bytes(rsp),
        Message:     &msg.Message{},
        Id:          id,
      }, c.Hub)
    }
  }
}

func broadcast(msg *msg.WsPacket, hub *WsHub) {
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
      w.Write(helper.Marshal2Bytes(packet))
      if err := w.Close(); err != nil {
        log.Error(err)
        return
      }
    case <-ticker.C:
      c.conn.SetWriteDeadline(time.Now().Add(writeWait))
      if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
        return
      }
    }
  }
}

var gExistErr = []byte(`exist!`)

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
      send:   make(chan *msg.WsPacket),
    }
    client.Hub.register <- client

    go client.writePump()
    go client.readPump()
    return
  }
  w.Write(gExistErr)
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
  send   chan *msg.WsPacket
}

type WsHub struct {
  Clients *sync.Map

  BroadcastList *sync.Map

  lock       *sync.Mutex
  maxIdl     int
  register   chan *Client
  unregister chan *Client
  broadcast  chan *msg.WsPacket
  invoking   *Invoking
}

type Invoking struct {
  Scheduler map[uint32]*SchedulerHandler
}

type SchedulerHandler struct {
  RequestType reflect.Type
  handler     Endpoint
}

type Endpoint func(
  client *Client,
  req interface{}) (rsp interface{}, wsId string, err error)

func (h *WsHub) RegisterEndpoint(serviceCode uint32, req proto.Message, endpoint Endpoint) {
  h.lock.Lock()
  if _, ok := h.invoking.Scheduler[serviceCode]; ok {
    panic("handler is already register")
  }
  h.invoking.Scheduler[serviceCode] = &SchedulerHandler{
    RequestType: reflect.TypeOf(req).Elem(),
    handler:     endpoint,
  }
  h.lock.Unlock()
}

func (i *Invoking) GetHandler(serviceCode uint32) (handler *SchedulerHandler, err error) {
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
    broadcast:     make(chan *msg.WsPacket),
    maxIdl:        maxIdl,
    invoking:      &Invoking{Scheduler: make(map[uint32]*SchedulerHandler)},
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
        h.Clients.Delete(client.UserId)
        close(client.send)
      }
    case packet := <-h.broadcast:
      if id, ok := h.BroadcastList.Load(packet.Id); ok {
        for _, v := range id.([]string) {
          if cnn, ok := h.Clients.Load(v); ok {
            cnn.(*Client).send <- packet
          }
        }
      }
    }
  }
  h.lock.Unlock()
}
