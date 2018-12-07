package hello_world

import (
  "github.com/jinbanglin/go-web/ws/exmple/proto"
  "github.com/jinbanglin/micro/message"
  "github.com/jinbanglin/go-web/ws"
  "strconv"
  "time"
  "github.com/rs/xid"
  "sync"
  "github.com/jinbanglin/helper"
  "github.com/go-redis/redis"
  "encoding/json"
)

func Hello(client *ws.Client, req interface{}) (rsp interface{}, wsId string, err error) {
  userLoad, sendPacket := req.(*hello.HelloReq), &hello.HelloRsp{Message: &msg.Message{
    Code: 200,
    Msg:  "SUCCESS",
  }, World: "world"}
  sendPacket.World += userLoad.Hello
  return sendPacket, userLoad.WsId, nil
}

func CreateRoom(client *ws.Client, req interface{}) (rsp interface{}, wsId string, err error) {
  client.WsId = strconv.FormatInt(time.Now().Unix(), 10) + "." + xid.New().String()
  client.Hub.Clients.Store(client.UserId, client)
  client.Hub.BroadcastList.Store(client.WsId, []string{client.UserId})
  return &hello.CreateRoomRsp{
    Message: &msg.Message{
      Code: 200,
      Msg:  "SUCCESS",
    },
    WsId: client.WsId,
  }, client.WsId, nil
}

var lock = new(sync.RWMutex)

func EntryRoom(client *ws.Client, req interface{}) (rsp interface{}, wsId string, err error) {
  userLoad, sendPacket := req.(*hello.EntryRoomReq), &hello.EntryRoomRsp{}
  lock.Lock()
  defer lock.Unlock()
  if wsId, ok := client.Hub.BroadcastList.Load(userLoad.WsId); ok {
    client.Hub.BroadcastList.Store(userLoad.WsId, append(wsId.([]string), client.UserId))
  }
  sendPacket.Message = &msg.Message{
    Code: 200,
    Msg:  "SUCCESS",
  }
  return
}

func EntryRoomRedis(client *ws.Client, req interface{}) (rsp interface{}, wsId string, err error) {
  userLoad, sendPacket := req.(*hello.EntryRoomReq), &hello.EntryRoomRsp{}
  helper.GRedisRing.Pipelined(func(pipeliner redis.Pipeliner) error {
    b, err := pipeliner.Get(userLoad.WsId).Bytes()
    if err != nil {
      sendPacket.Message = &msg.Message{
        Code: 500,
        Msg:  "FAIL",
      }
    }
    var result []string
    json.Unmarshal(b, &result)
    pipeliner.Set(userLoad.WsId, helper.Marshal2Bytes(append(result, client.UserId)), time.Hour*24)
    return nil
  })
  sendPacket.Message = &msg.Message{
    Code: 200,
    Msg:  "SUCCESS",
  }
  return
}
