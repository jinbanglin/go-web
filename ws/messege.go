package ws

import "github.com/jinbanglin/micro/message"

var MsgSuccessMessage = msg.Message{Code: 200, Msg: "SUCCESS"}

var MsgServerError = msg.Message{Code: 500, Msg: "FAIL"}

var MsgClientError = msg.Message{Code: 400, Msg: "CLIENT FAIL"}
