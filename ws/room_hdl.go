package ws

import (
  "strconv"
  "time"
  "github.com/rs/xid"
  "github.com/jinbanglin/go-web/ws/proto"
)

func Hello(client *Client, req interface{}) (rsp interface{}, err error) {
  userLoad, sendPacket := req.(*room.HelloReq), &room.HelloRsp{Message: &MsgSuccessMessage, World: "world"}
  sendPacket.World += userLoad.Hello
  return sendPacket, nil
}

func CreateRoom(client *Client, req interface{}) (rsp interface{}, err error) {
  userLoad, sendPacket := req.(*room.CreateRoomReq), &room.CreateRoomRsp{Message: &MsgSuccessMessage}
  client.RoomId = strconv.FormatInt(time.Now().Unix(), 10) + "." + xid.New().String()
  client.Cid = client.UserId + "." + xid.New().String()
  client.Expire = userLoad.Duration
  client.SetState()
  client.Hub.Clients.Store(client.UserId, client)
  if err = client.Hub.SetRoom(client.RoomId, []string{client.Cid}, time.Duration(userLoad.Duration)*time.Second); err != nil {
    sendPacket.Message = &MsgServerError
  }
  return sendPacket, err
}

func EntryRoom(client *Client, req interface{}) (rsp interface{}, err error) {
  userLoad, sendPacket := req.(*room.EntryRoomReq), &room.EntryRoomRsp{Message: &MsgSuccessMessage}
  cidNew := client.UserId + "." + xid.New().String()
  client.EntryRoom(userLoad.RoomId, cidNew)
  return sendPacket, nil
}
