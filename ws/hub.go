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
  "strconv"
  "github.com/rs/xid"
  "github.com/spf13/viper"
)

var (
  // Time allowed to write a message to the peer.
  WriteWait = 10 * time.Second

  // Time allowed to read the next pong message from the peer.
  PongWait = 60 * time.Second

  // send pings to peer with this period. Must be less than PongWait.
  PingPeriod = (PongWait * 9) / 10

  // Maximum message size allowed from peer.
  MaxMessageSize int64 = 1024

  // service code size
  ServiceCodeSize = 5

  RedisKey4WsRoomUids = "websocket:room:info:uids:"

  DsyncLockTimeExpire = time.Second * 10
)

func WsChaos() {
  if v := viper.GetInt("ws.write_wait"); v > 0 {
    WriteWait = time.Duration(v) * time.Second
  }
  if v := viper.GetInt("ws.pong_wait"); v > 0 {
    PongWait = time.Duration(v) * time.Second
  }
  if v := viper.GetInt64("ws.max_message_size"); v > 0 {
    MaxMessageSize = v
  }
  if v := viper.GetInt("ws.service_code_size"); v > 0 {
    ServiceCodeSize = v
  }
}

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
        log.Error(err, t)
      }
      if c.GetState() != 1 {
        c.Hub.unregister <- c
      }

      break
    }
    log.Debugf("FROM |user_id=%s", c.UserId, helper.Byte2String(packet))
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

    log.Debugf("BROADCAST |user_id=%s", c.UserId, helper.Marshal2String(rsp))

    Broadcast(&broadcastData{
      roomId: c.RoomId,
      userId: c.UserId,
      data:   bytesCombine(packet[0:ServiceCodeSize], helper.Marshal2Bytes(rsp)),
    }, c.Hub)
  }
  c.conn.Close()
}

func bytesCombine(pBytes ...[]byte) []byte {
  return bytes.Join(pBytes, []byte(""))
}

func Broadcast(msg *broadcastData, hub *WsHub) {
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

func Handshake(hub *WsHub, userId string, w http.ResponseWriter, r *http.Request) {
  conn, err := gUpGrader.Upgrade(w, r, nil)
  if err != nil {
    log.Error(err)
    w.Write(upgradeError)
    return
  }

  client, ok := Search(hub, userId)
  if ok {
    client.kicked()
    client.conn = conn
    client.SetState()
  } else {
    client = &Client{
      UserId: userId,
      Hub:    hub,
      conn:   conn,
      send:   make(chan []byte),
    }
    client.Hub.register <- client
    client = &Client{
      UserId: userId,
      Hub:    hub,
      conn:   conn,
      send:   make(chan []byte),
    }
    client.Hub.register <- client
  }
}

func Search(hub *WsHub, userId string) (*Client, bool) {
  v, ok := hub.Clients.Load(userId)
  if ok {
    return v.(*Client), ok
  }
  return nil, false
}

type Client struct {
  UserId    string
  RoomId    string //room roomId for find user list to Broadcast
  Hub       *WsHub
  conn      *websocket.Conn
  send      chan []byte
  State     int32 //0:normal,1:had connected,need update but no unregister
  IsInARoom bool
}

type WsHub struct {
  Clients *sync.Map

  lock       *sync.Mutex
  register   chan *Client
  unregister chan *Client
  broadcast  chan *broadcastData
  invoking   *Invoking

  clock *clock.Clock
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
    log.Error(" |no such service code :", serviceCode)
    return nil, errors.New("no service")
  }
  return
}

var GHub *WsHub

func SetupWEBSocketHub() {
  GHub = &WsHub{
    lock:       new(sync.Mutex),
    Clients:    new(sync.Map),
    register:   make(chan *Client),
    unregister: make(chan *Client),
    broadcast:  make(chan *broadcastData),
    invoking:   &Invoking{Scheduler: make(map[string]*SchedulerHandler)},
    clock:      clock.NewClock(),
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
        client.RemoveUser4Room()
      }
    case packet := <-h.broadcast:
      roomData, _ := h.GetRoom(packet.roomId)

      log.Debugf("TO |user_ids=%v", roomData, helper.Byte2String(packet.data))
      if len(roomData.UserIds) > 0 {
        for _, v := range roomData.UserIds {
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

var kickedMsg = []byte(`40001{"message":{"code":401,"msg":"you've been kicked"}}`)

func (c *Client) kicked() {
  c.send <- kickedMsg
}

//-------------------------------chat demo-----------------------

type RoomData struct {
  ExpireAt time.Time
  UserIds  []string
}

func (c *Client) ExitRoom() {
  c.RemoveUser4Room()
  c.RoomId = ""
  c.IsInARoom = false
}

var exitMsg = `40002{"message":{"code":402,"msg":"exit room"},"data":{"user_id":"%s"}}`

func (c *Client) RemoveUser4Room() {
  if !c.IsInARoom {
    return
  }
  Broadcast(&broadcastData{
    roomId: c.RoomId,
    userId: c.UserId,
    data:   helper.String2Byte(fmt.Sprintf(exitMsg, c.UserId)),
  }, c.Hub)
  lock := helper.NewDLock(c.RoomId, DsyncLockTimeExpire)
  lock.Lock()
  defer lock.Unlock()
  roomData, err := GHub.GetRoom(c.RoomId)
  if err != nil {
    log.Debug(err)
    return
  }
  var index = 0
  for k, v := range roomData.UserIds {
    if strings.EqualFold(v, c.UserId) {
      index = k
      break
    }
  }
  roomData.UserIds = append(roomData.UserIds[:index], roomData.UserIds[index+1:]...)
  GHub.SetRoomByRoomById(c.RoomId, roomData.UserIds, roomData.ExpireAt)
}

func (h *WsHub) GetRoom(roomId string) (roomData *RoomData, err error) {
  b, err := helper.GRedisRing.Get(RedisKey4WsRoomUids + roomId).Bytes()
  if err != nil {
    log.Debug(err)
    return
  }
  roomData = &RoomData{}
  err = json.Unmarshal(b, &roomData)
  return
}

func (h *WsHub) SetRoomByRoomById(roomId string, userIds []string, t time.Time) error {
  return helper.SetExpireAt(roomId, helper.Marshal2Bytes(&RoomData{UserIds: userIds, ExpireAt: t}), t)
}

func (c *Client) CreateRoom(d time.Duration) error {
  c.IsInARoom = true
  return c.Hub.SetRoomByRoomById(c.MakeRoomId(), []string{c.UserId}, time.Now().Add(d))
}

func (c *Client) EntryRoomByRoomId(roomId string) error {
  lock := helper.NewDLock(roomId, DsyncLockTimeExpire)
  lock.Lock()
  defer lock.Unlock()

  roomInfo, err := GHub.GetRoom(roomId)
  if (err != nil && err == redis.Nil) || err == nil {
    var exist = false
    for _, v := range roomInfo.UserIds {
      if strings.EqualFold(c.UserId, v) {
        exist = true
      }
    }
    if !exist {
      roomInfo.UserIds = append(roomInfo.UserIds, c.UserId)
    }
    c.RoomId = roomId
    c.IsInARoom = true
    if err = GHub.SetRoomByRoomById(roomId, roomInfo.UserIds, roomInfo.ExpireAt); err == nil {
      c.IsInARoom = true
      return nil
    }
  }
  return err
}

func (c *Client) GetState() int32 {
  return atomic.LoadInt32(&c.State)
}

func (c *Client) SetState() {
  atomic.SwapInt32(&c.State, 1)
}

func (c *Client) MakeRoomId() string {
  return strconv.FormatInt(time.Now().Unix(), 10) + "." + xid.New().String()
}
