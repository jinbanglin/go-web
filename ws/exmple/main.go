/*
 * Copyright (c) 2018 All Rights Reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * Please contact me:
 * Author:jinbanglin
 * File:main.go
 * EMAIL:570751295@qq.com
 * LastModified:2018/08/01 11:45:01
 */

package main

import (
  "github.com/gin-gonic/gin"
  "github.com/jinbanglin/log"
  "github.com/jinbanglin/go-web"
  "github.com/jinbanglin/go-web/ws"
  "github.com/jinbanglin/go-web/ws/exmple/proto"
  "github.com/jinbanglin/go-web/ws/exmple/hello_world"
)

func main() {
  service := web.NewService(
    web.Name("go.micro.web.hello"),
  )

  if err := service.Init(); err != nil {
    log.Fatal("Init", err)
  }
  app := gin.Default()
  ws.SetupWEBSocketHub(1024,1)
  ws.GHub.RegisterEndpoint("10000", &hello.HelloReq{}, hello_world.Hello)
  ws.GHub.RegisterEndpoint("10001", &hello.CreateRoomReq{}, hello_world.CreateRoom)
  ws.GHub.RegisterEndpoint("10002", &hello.EntryRoomReq{}, hello_world.EntryRoom)
  ws.GHub.RegisterEndpoint("10003", &hello.EntryRoomReq{}, hello_world.EntryRoomRedis)

  app.POST("/ws/:userid", func(context *gin.Context) {
    ws.WSUpgrade(ws.GHub, context.Param("userid"), context.Writer, context.Request)
  })
  if err := service.Run(); err != nil {
    log.Fatal(err)
  }
}
