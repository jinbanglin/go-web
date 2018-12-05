package ws

import (
  "sync"
  "context"
  "reflect"
  "github.com/gogo/protobuf/proto"
  "github.com/jinbanglin/log"
  "errors"
  "github.com/gorilla/websocket"
  "time"
  "github.com/jinbanglin/helper"
  "net/http"
  "github.com/json-iterator/go"
  "github.com/jinbanglin/micro/message"
  "github.com/rs/xid"
  "strconv"
)

const (
  // Time allowed to write a message to the peer.
  writeWait = 10 * time.Second

  // Time allowed to read the next pong message from the peer.
  pongWait = 60 * time.Second

  // Send pings to peer with this period. Must be less than pongWait.
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

func (c *Client) readPump() {
  defer func() {
    c.hub.unregister <- c
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
      s, err := c.hub.invoking.GetHandler(userLoad.ServiceCode)
      if err != nil {
        log.Error(err)
        break
      }
      req := reflect.New(s.RequestType).Interface().(proto.Message)
      if err = jsoniter.Unmarshal(userLoad.Payload, req); err != nil {
        log.Error(err)
        break
      }
      rsp, err := s.handler(context.TODO(), c.userId, req)
      if err != nil {
        log.Error(err)
        break
      }
      Broadcast(&msg.WsPacket{
        ServiceCode: userLoad.ServiceCode,
        Payload:     helper.Marshal2Bytes(rsp),
        Message:     &msg.Message{},
        Id:          userLoad.Id,
      }, c.hub)
    }
  }
}

//todo: 广播给所有人
func Broadcast(msg *msg.WsPacket, hub *Hub) {
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

func WSUpgrade(hub *Hub, userId string, w http.ResponseWriter, r *http.Request) {
  client, ok := search(hub, userId)
  if !ok {
    conn, err := gUpGrader.Upgrade(w, r, nil)
    if err != nil {
      log.Error(err)
      return
    }
    client = &Client{
      userId: userId,
      hub:    hub,
      conn:   conn,
      send:   make(chan *msg.WsPacket),
    }
    client.hub.register <- client

    go client.writePump()
    go client.readPump()
  }
}

var gConnectErr = []byte(`connect timeout!`)

func WSCreateRoom(hub *Hub, userId string, w http.ResponseWriter, r *http.Request) {
  client, ok := search(hub, userId)
  if !ok {
    w.Write(gConnectErr)
    return
  }
  client.wid = strconv.FormatInt(time.Now().Unix(), 64) + "." + xid.New().String()
  client.hub.broadcastList.Store(client.wid, []string{userId})
  w.Write(helper.String2Byte(client.wid))
}

func search(hub *Hub, userId string) (*Client, bool) {
  v, ok := hub.clients.Load(userId)
  if ok {
    return v.(*Client), ok
  }
  return nil, false
}

type Client struct {
  userId string
  wid    string
  hub    *Hub
  conn   *websocket.Conn
  send   chan *msg.WsPacket
}

type Hub struct {
  lock          *sync.Mutex
  maxIdl        int
  clients       *sync.Map
  register      chan *Client
  unregister    chan *Client
  broadcast     chan *msg.WsPacket
  broadcastList *sync.Map
  invoking      *Invoking
}

type Invoking struct {
  Scheduler map[uint32]*SchedulerHandler
}

type SchedulerHandler struct {
  RequestType reflect.Type
  handler     Endpoint
}

type Endpoint func(ctx context.Context, userId string, req interface{}) (rsp interface{}, err error)

func (h *Hub) RegisterEndpoint(serviceCode uint32, req proto.Message, endpoint Endpoint) {
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

var GHub *Hub

func SetupWEBSocketHub(maxIdl int) {
  GHub = &Hub{
    lock:          new(sync.Mutex),
    clients:       new(sync.Map),
    broadcastList: new(sync.Map),
    register:      make(chan *Client, maxIdl),
    unregister:    make(chan *Client),
    broadcast:     make(chan *msg.WsPacket),
    maxIdl:        maxIdl,
    invoking:      &Invoking{Scheduler: make(map[uint32]*SchedulerHandler)},
  }
  go GHub.Run()
}

func (h *Hub) Run() {
  h.lock.Lock()
  for {
    select {
    case client := <-h.register:
      h.clients.Store(client.userId, client)
    case client := <-h.unregister:
      if _, ok := h.clients.Load(client.userId); ok {
        h.clients.Delete(client.userId)
        close(client.send)
      }
    case packet := <-h.broadcast:
      if id, ok := h.broadcastList.Load(packet.Id); ok {
        for _, v := range id.([]string) {
          if cnn, ok := h.clients.Load(v); ok {
            cnn.(*Client).send <- packet
          }
        }
      }
    }
  }
  h.lock.Unlock()
}
