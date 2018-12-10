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
  "github.com/go-redis/redis"
  "strings"
  "sync/atomic"
  "github.com/jinbanglin/go-web/ws/proto"
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

  RedisKey4WsRoom = "websocket:room:info:"

  DsyncLockTimeExpire = time.Second * 3
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
  c.setBase()
  for {
    t, packet, err := c.conn.ReadMessage()
    if err != nil {
      if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
        log.Debug(err, t)
      }
      break
    }
    serviceCode := helper.Byte2String(packet[0:ServiceCodeSize])
    s, err := c.Hub.invoking.GetHandler(serviceCode)
    if err != nil {
      log.Error(err)
      continue
    }
    req := reflect.New(s.RequestType).Interface().(proto.Message)
    if err = json.Unmarshal(packet[ServiceCodeSize:], req); err != nil {
      log.Error(err)
      continue
    }
    rsp, err := s.handler(c, req)
    if err != nil {
      log.Error(err)
      continue
    }
    broadcast(&broadcastData{
      roomId: c.RoomId,
      userId: c.UserId,
      data:   bytesCombine(packet[0:ServiceCodeSize], helper.Marshal2Bytes(rsp)),
    }, c.Hub)
  }
  c.Hub.unregister <- c
  c.conn.Close()
}

func bytesCombine(pBytes ...[]byte) []byte {
  return bytes.Join(pBytes, []byte(""))
}

func broadcast(msg *broadcastData, hub *WsHub) {
  hub.broadcast <- msg
}

func (c *Client) writePump() {
  defer func() {
    c.conn.Close()
  }()
  var clockQuit = make(chan struct{})
  var job clock.Job
  job, _ = c.Hub.clock.AddJobRepeat(PingPeriod, 0, func() {
    c.conn.SetWriteDeadline(time.Now().Add(WriteWait))
    if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
      log.Debug(err)
      clockQuit <- struct{}{}
    }
  })
  for {
    select {
    case packet, ok := <-c.send:
      c.conn.SetWriteDeadline(time.Now().Add(WriteWait))
      if !ok {
        c.conn.WriteMessage(websocket.CloseMessage, []byte{})
        job.Cancel()
        return
      }
      w, err := c.conn.NextWriter(websocket.TextMessage)
      if err != nil {
        log.Debug(err)
        job.Cancel()
        return
      }
      w.Write(packet)
      if err := w.Close(); err != nil {
        log.Debug(err)
        job.Cancel()
        return
      }
    case <-clockQuit:
      job.Cancel()
      return
    }
  }
}

var upgradeError = []byte(`50001{"message":{"code":500,"msg":"upgrade websocket fail"}}`)

func WSUpgrade(hub *WsHub, userId string, w http.ResponseWriter, r *http.Request) {
  client, ok := Search(hub, userId)
  if ok {
    client.exit()
  }
  conn, err := gUpGrader.Upgrade(w, r, nil)
  if err != nil {
    log.Error(err)
    w.Write(upgradeError)
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

func Search(hub *WsHub, userId string) (*Client, bool) {
  v, ok := hub.Clients.Load(userId)
  if ok {
    return v.(*Client), ok
  }
  return nil, false
}

type Client struct {
  UserId string
  Cid    string //client roomId
  RoomId string //room roomId for find user list to broadcast
  Hub    *WsHub
  conn   *websocket.Conn
  send   chan []byte
  State  int32 //1 in a room
  Expire int64
}

func (c *Client) resetCid(cid string) {

}

type WsHub struct {
  Clients *sync.Map

  lock       *sync.Mutex
  register   chan *Client
  unregister chan *Client
  broadcast  chan *broadcastData
  invoking   *Invoking

  clock *clock.Clock

  ring *redis.Ring
}

type broadcastData struct {
  roomId string
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

func SetupWEBSocketHub(def bool) {
  GHub = &WsHub{
    lock:       new(sync.Mutex),
    Clients:    new(sync.Map),
    register:   make(chan *Client),
    unregister: make(chan *Client),
    broadcast:  make(chan *broadcastData),
    invoking:   &Invoking{Scheduler: make(map[string]*SchedulerHandler)},
    clock:      clock.NewClock(),
  }
  if GHub.redisConfig() != nil {
    panic("please check your redis config")
  }
  if def {
    GHub.RegisterEndpoint("10000", &room.CreateRoomReq{}, CreateRoom)
    GHub.RegisterEndpoint("10001", &room.EntryRoomReq{}, EntryRoom)
    GHub.RegisterEndpoint("10002", &room.HelloReq{}, Hello)
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
        client.RemoveUser(client.RoomId, client.Cid)
      }
    case packet := <-h.broadcast:
      cids, _ := h.GetRoom(packet.roomId)
      if len(cids) > 0 {
        for _, v := range cids {
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

var exitMsg = []byte(`10001{"message":{"code":200,"msg":"SUCCESS"}}`)

func (c *Client) exit() {
  c.Hub.Clients.Delete(c.Cid)
  c.send <- exitMsg
}

func (h *WsHub) redisConfig() error {
  h.ring = helper.GRedisRing
  if h.ring == nil || h.ring.Ping().Err() != nil {
    return errors.New("redis config error and ping fail")
  }
  return nil
}

func (h *WsHub) GetRoom(roomId string) (result []string, err error) {
  b, err := h.ring.Get(RedisKey4WsRoom + roomId).Bytes()
  if err != nil {
    log.Debug(err)
    return
  }
  err = json.Unmarshal(b, &result)
  return
}

func (h *WsHub) SetRoom(roomId string, cid []string, duration time.Duration) error {
  return h.ring.Set(RedisKey4WsRoom+roomId, helper.Marshal2Bytes(&cid), duration).Err()
}

func (c *Client) EntryRoom(roomId, cidNew string) error {
  lock := helper.NewDLock(roomId, DsyncLockTimeExpire)
  lock.Lock()
  defer lock.Unlock()

  if c.GetState() == 1 {
    return nil
  }
  roomInfo, err := GHub.GetRoom(roomId)
  if (err != nil && err == redis.Nil) || err == nil {
    var exist = false
    for k, v := range roomInfo {
      if strings.EqualFold(c.Cid, v) {
        roomInfo[k] = cidNew
        exist = true
      }
    }
    if !exist {
      roomInfo = append(roomInfo, cidNew)
    }
    c.Cid = cidNew
    c.RoomId = roomId
    c.Update()
    return GHub.SetRoom(roomId, roomInfo, time.Duration(c.Expire))
  } else {
    return err
  }
}

func (c *Client) GetState() int32 {
  return atomic.LoadInt32(&c.State)
}

func (c *Client) SetState() {
  atomic.SwapInt32(&c.State, 1)
}

func (c *Client) resetState() {
  atomic.CompareAndSwapInt32(&c.State, 1, 0)
}

func (c *Client) Update() {
  c.Hub.Clients.Store(c.Cid, c)
}

func (c *Client) RemoveUser(roomId, cidOld string) {
  if c.GetState() != 1 {
    return
  }
  lock := helper.NewDLock(roomId, DsyncLockTimeExpire)
  lock.Lock()
  defer lock.Unlock()
  cids, err := GHub.GetRoom(roomId)
  if err != nil {
    log.Debug(err)
    return
  }
  var index = 0
  for k, v := range cids {
    if strings.EqualFold(v, cidOld) {
      index = k
      break
    }
  }
  cids = append(cids[:index], cids[index+1:]...)
  GHub.SetRoom(roomId, cids, DsyncLockTimeExpire)
  c.resetState()
}
