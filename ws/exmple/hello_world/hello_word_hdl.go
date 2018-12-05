package hello_world

import (
  "context"
  "github.com/jinbanglin/go-web/ws/exmple/proto"
  "github.com/jinbanglin/micro/message"
)

func Hello(ctx context.Context, userId string, req interface{}) (rsp interface{}, err error) {
  userLoad, sendPacket := req.(*hello.HelloReq), &hello.HelloRsp{Message: &msg.Message{
    Code: 200,
    Msg:  "SUCCESS",
  }, World: "world"}
  sendPacket.World += userLoad.Hello
  return sendPacket, nil
}
