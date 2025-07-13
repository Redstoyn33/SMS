package main

import (
	"SMS/internal/mqtt"
	"log"
	"net"
)

func main() {
	broker := mqtt.NewBroker()
	port := ":1883"

	ln, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Ошибка при запуске сервера: %v", err)
	}
	defer ln.Close()

	log.Printf("MQTT брокер запущен на порту %s", port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Ошибка при принятии соединения: %v", err)
			continue
		}
		go broker.HandleConnection(conn)
	}
}
