package room_manager

import (
  "github.com/jinbanglin/go-web/ws"
  "strconv"
  "time"
  "github.com/rs/xid"
  "github.com/jinbanglin/go-web/ws/room/proto"
)

func Hello(client *ws.Client, req interface{}) (rsp interface{}, err error) {
  userLoad, sendPacket := req.(*room.HelloReq), &room.HelloRsp{Message: &MsgSuccessMessage, World: "world"}
  sendPacket.World += userLoad.Hello
  return sendPacket, nil
}

func CreateRoom(client *ws.Client, req interface{}) (rsp interface{}, err error) {
  userLoad, sendPacket := req.(*room.CreateRoomReq), &room.CreateRoomRsp{Message: &MsgSuccessMessage}
  client.RoomId = strconv.FormatInt(time.Now().Unix(), 10) + "." + xid.New().String()
  client.Cid = client.UserId + "." + xid.New().String()
  client.Expire = userLoad.Duration
  client.SetState()
  client.Hub.Clients.Store(client.UserId, client)
  if err = client.SetRoom(client.RoomId, []string{client.Cid}, time.Duration(userLoad.Duration)*time.Second); err != nil {
    sendPacket.Message = &MsgServerError
  }
  return sendPacket, err
}

func EntryRoom(client *ws.Client, req interface{}) (rsp interface{}, err error) {
  userLoad, sendPacket := req.(*room.EntryRoomReq), &room.EntryRoomRsp{Message: &MsgSuccessMessage}
  cidNew := client.UserId + "." + xid.New().String()
  client.EntryRoom(userLoad.RoomId, cidNew)
  return sendPacket, nil
}
