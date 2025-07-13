package mqtt

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

var ErrMessageTooLarge = fmt.Errorf("mqtt: message size exceeds %dK", MaxMessageSize/1024)
var ErrMessageBadPacket = errors.New("mqtt: bad packet")

const (
	maxHeaderSize  = 6
	MaxMessageSize = 65536
)

type Message interface {
	fmt.Stringer
	Type() uint8
	EncodeTo(w io.Writer) (int, error)
}

type Reader interface {
	io.Reader
	ReadByte() (byte, error)
}

const (
	TypeOfConnect = uint8(iota + 1)
	TypeOfConnack
	TypeOfPublish
	TypeOfPuback
	TypeOfPubrec
	TypeOfPubrel
	TypeOfPubcomp
	TypeOfSubscribe
	TypeOfSuback
	TypeOfUnsubscribe
	TypeOfUnsuback
	TypeOfPingreq
	TypeOfPingresp
	TypeOfDisconnect
)

type Header struct {
	DUP    bool
	Retain bool
	QOS    uint8
}

type Connect struct {
	ProtoName      []byte
	Version        uint8
	UsernameFlag   bool
	PasswordFlag   bool
	WillRetainFlag bool
	WillQOS        uint8
	WillFlag       bool
	CleanSeshFlag  bool
	KeepAlive      uint16
	ClientID       []byte
	WillTopic      []byte
	WillMessage    []byte
	Username       []byte
	Password       []byte
}

type Connack struct {
	ReturnCode uint8
}

type Publish struct {
	Header
	Topic     []byte
	MessageID uint16
	Payload   []byte
}

type Puback struct {
	MessageID uint16
}

type Pubrec struct {
	MessageID uint16
}

type Pubrel struct {
	MessageID uint16
	Header    Header
}

type Pubcomp struct {
	MessageID uint16
}

type Subscribe struct {
	Header
	MessageID     uint16
	Subscriptions []TopicQOSTuple
}

type Suback struct {
	MessageID uint16
	Qos       []uint8
}

type Unsubscribe struct {
	Header
	MessageID uint16
	Topics    []TopicQOSTuple
}

type Unsuback struct {
	MessageID uint16
}

type Pingreq struct {
}

type Pingresp struct {
}

type Disconnect struct {
}

type TopicQOSTuple struct {
	Qos   uint8
	Topic []byte
}

func DecodePacket(rdr Reader, maxMessageSize int64) (Message, error) {
	hdr, sizeOf, messageType, err := decodeHeader(rdr)
	if err != nil {
		return nil, err
	}

	switch messageType {
	case TypeOfPingreq:
		return &Pingreq{}, nil
	case TypeOfPingresp:
		return &Pingresp{}, nil
	case TypeOfDisconnect:
		return &Disconnect{}, nil
	}

	if int64(sizeOf) > maxMessageSize {
		return nil, ErrMessageTooLarge
	}

	buffer := make([]byte, sizeOf)
	_, err = io.ReadFull(rdr, buffer)
	if err != nil {
		return nil, err
	}

	var msg Message
	switch messageType {
	case TypeOfConnect:
		msg, err = decodeConnect(buffer)
	case TypeOfConnack:
		msg = decodeConnack(buffer, hdr)
	case TypeOfPublish:
		msg, err = decodePublish(buffer, hdr)
	case TypeOfPuback:
		msg = decodePuback(buffer)
	case TypeOfPubrec:
		msg = decodePubrec(buffer)
	case TypeOfPubrel:
		msg = decodePubrel(buffer, hdr)
	case TypeOfPubcomp:
		msg = decodePubcomp(buffer)
	case TypeOfSubscribe:
		msg, err = decodeSubscribe(buffer, hdr)
	case TypeOfSuback:
		msg = decodeSuback(buffer)
	case TypeOfUnsubscribe:
		msg, err = decodeUnsubscribe(buffer, hdr)
	case TypeOfUnsuback:
		msg = decodeUnsuback(buffer)
	default:
		return nil, fmt.Errorf("Invalid zero-length packet with type %d", messageType)
	}

	return msg, err
}

func (c *Connect) EncodeTo(w io.Writer) (int, error) {
	return 0, fmt.Errorf("connect encode not implemented for broker")
}

func (c *Connect) Type() uint8 {
	return TypeOfConnect
}

func (c *Connect) String() string {
	return "connect"
}

func (c *Connack) EncodeTo(w io.Writer) (int, error) {
	array := make([]byte, maxHeaderSize+2)

	head := array[0:maxHeaderSize]
	buf := array[maxHeaderSize:]

	offset := writeUint8(buf, byte(0))
	offset += writeUint8(buf[offset:], c.ReturnCode)

	start := writeHeader(head, TypeOfConnack, nil, offset)
	return w.Write(array[start : maxHeaderSize+offset])
}

func (c *Connack) Type() uint8 {
	return TypeOfConnack
}

func (c *Connack) String() string {
	return "connack"
}

func (p *Publish) EncodeTo(w io.Writer) (int, error) {
	array := make([]byte, MaxMessageSize)
	head := array[0:maxHeaderSize]
	buf := array[maxHeaderSize:]

	length := 2 + len(p.Topic) + len(p.Payload)
	if p.QOS > 0 {
		length += 2
	}

	if length > MaxMessageSize {
		return 0, ErrMessageTooLarge
	}

	offset := writeString(buf, p.Topic)
	if p.Header.QOS > 0 {
		offset += writeUint16(buf[offset:], p.MessageID)
	}

	copy(buf[offset:], p.Payload)
	offset += len(p.Payload)

	start := writeHeader(head, TypeOfPublish, &p.Header, offset)
	return w.Write(array[start : maxHeaderSize+offset])
}

func (p *Publish) Type() uint8 {
	return TypeOfPublish
}

func (p *Publish) String() string {
	return "pub"
}

func (p *Puback) EncodeTo(w io.Writer) (int, error) {
	return 0, fmt.Errorf("puback encode not implemented for broker")
}

func (p *Puback) Type() uint8 {
	return TypeOfPuback
}

func (p *Puback) String() string {
	return "puback"
}

func (p *Pubrec) EncodeTo(w io.Writer) (int, error) {
	return 0, fmt.Errorf("pubrec encode not implemented for broker")
}

func (p *Pubrec) Type() uint8 {
	return TypeOfPubrec
}

func (p *Pubrec) String() string {
	return "pubrec"
}

func (p *Pubrel) EncodeTo(w io.Writer) (int, error) {
	return 0, fmt.Errorf("pubrel encode not implemented for broker")
}

func (p *Pubrel) Type() uint8 {
	return TypeOfPubrel
}

func (p *Pubrel) String() string {
	return "pubrel"
}

func (p *Pubcomp) EncodeTo(w io.Writer) (int, error) {
	return 0, fmt.Errorf("pubcomp encode not implemented for broker")
}

func (p *Pubcomp) Type() uint8 {
	return TypeOfPubcomp
}

func (p *Pubcomp) String() string {
	return "pubcomp"
}

func (s *Subscribe) EncodeTo(w io.Writer) (int, error) {
	return 0, fmt.Errorf("subscribe encode not implemented for broker")
}

func (s *Subscribe) Type() uint8 {
	return TypeOfSubscribe
}

func (s *Subscribe) String() string {
	return "sub"
}

func (s *Suback) EncodeTo(w io.Writer) (int, error) {
	array := make([]byte, maxHeaderSize+2+len(s.Qos))

	head := array[0:maxHeaderSize]
	buf := array[maxHeaderSize:]

	offset := writeUint16(buf, s.MessageID)
	for _, q := range s.Qos {
		offset += writeUint8(buf[offset:], byte(q))
	}

	start := writeHeader(head, TypeOfSuback, nil, offset)
	return w.Write(array[start : maxHeaderSize+offset])
}

func (s *Suback) Type() uint8 {
	return TypeOfSuback
}

func (s *Suback) String() string {
	return "suback"
}

func (u *Unsubscribe) EncodeTo(w io.Writer) (int, error) {
	return 0, fmt.Errorf("unsubscribe encode not implemented for broker")
}

func (u *Unsubscribe) Type() uint8 {
	return TypeOfUnsubscribe
}

func (u *Unsubscribe) String() string {
	return "unsub"
}

func (u *Unsuback) EncodeTo(w io.Writer) (int, error) {
	return 0, fmt.Errorf("unsuback encode not implemented for broker")
}

func (u *Unsuback) Type() uint8 {
	return TypeOfUnsuback
}

func (u *Unsuback) String() string {
	return "unsuback"
}

func (p *Pingreq) EncodeTo(w io.Writer) (int, error) {
	return w.Write([]byte{0xc0, 0x0})
}

func (p *Pingreq) Type() uint8 {
	return TypeOfPingreq
}

func (p *Pingreq) String() string {
	return "pingreq"
}

func (p *Pingresp) EncodeTo(w io.Writer) (int, error) {
	return w.Write([]byte{0xd0, 0x0})
}

func (p *Pingresp) Type() uint8 {
	return TypeOfPingresp
}

func (p *Pingresp) String() string {
	return "pingresp"
}

func (d *Disconnect) EncodeTo(w io.Writer) (int, error) {
	return w.Write([]byte{0xe0, 0x0})
}

func (d *Disconnect) Type() uint8 {
	return TypeOfDisconnect
}

func (d *Disconnect) String() string {
	return "disconnect"
}

func decodeHeader(rdr Reader) (hdr Header, length uint32, messageType uint8, err error) {
	firstByte, err := rdr.ReadByte()
	if err != nil {
		return Header{}, 0, 0, err
	}

	messageType = (firstByte & 0xf0) >> 4

	switch messageType {
	case TypeOfPublish, TypeOfSubscribe, TypeOfUnsubscribe, TypeOfPubrel:
		DUP := firstByte&0x08 > 0
		QOS := (firstByte & 0x06) >> 1
		retain := firstByte&0x01 > 0

		hdr = Header{
			DUP:    DUP,
			QOS:    QOS,
			Retain: retain,
		}
	}

	multiplier := uint32(1)
	digit := byte(0x80)

	for (digit & 0x80) != 0 {
		b, err := rdr.ReadByte()
		if err != nil {
			return Header{}, 0, 0, err
		}

		digit = b
		length += uint32(digit&0x7f) * multiplier
		multiplier *= 128

		if multiplier > 2097152 {
			return Header{}, 0, 0, ErrMessageTooLarge
		}
	}

	return hdr, uint32(length), messageType, nil
}

func decodeConnect(data []byte) (Message, error) {
	bookmark := uint32(0)

	protoname, err := readString(data, &bookmark)
	if err != nil {
		return nil, err
	}
	if bookmark >= uint32(len(data)) {
		return nil, ErrMessageBadPacket
	}
	ver := uint8(data[bookmark])
	bookmark++
	if bookmark >= uint32(len(data)) {
		return nil, ErrMessageBadPacket
	}
	flags := data[bookmark]
	bookmark++
	if bookmark+1 >= uint32(len(data)) {
		return nil, ErrMessageBadPacket
	}
	keepalive := readUint16(data, &bookmark)
	cliID, err := readString(data, &bookmark)
	if err != nil {
		return nil, err
	}
	connect := &Connect{
		ProtoName:      protoname,
		Version:        ver,
		KeepAlive:      keepalive,
		ClientID:       cliID,
		UsernameFlag:   flags&(1<<7) > 0,
		PasswordFlag:   flags&(1<<6) > 0,
		WillRetainFlag: flags&(1<<5) > 0,
		WillQOS:        (flags & (1 << 4) >> 3) | ((flags & (1 << 3)) >> 3),
		WillFlag:       flags&(1<<2) > 0,
		CleanSeshFlag:  flags&(1<<1) > 0,
	}

	if connect.WillFlag {
		if connect.WillTopic, err = readString(data, &bookmark); err != nil {
			return nil, err
		}
		if connect.WillMessage, err = readString(data, &bookmark); err != nil {
			return nil, err
		}
	}

	if connect.UsernameFlag {
		if connect.Username, err = readString(data, &bookmark); err != nil {
			return nil, err
		}
	}

	if connect.PasswordFlag {
		if connect.Password, err = readString(data, &bookmark); err != nil {
			return nil, err
		}
	}
	return connect, nil
}

func decodeConnack(data []byte, _ Header) Message {
	bookmark := uint32(1)
	retcode := data[bookmark]

	return &Connack{
		ReturnCode: retcode,
	}
}

func decodePublish(data []byte, hdr Header) (Message, error) {
	bookmark := uint32(0)
	topic, err := readString(data, &bookmark)
	if err != nil {
		return nil, err
	}
	var msgID uint16
	if hdr.QOS > 0 {
		if bookmark+1 >= uint32(len(data)) {
			return nil, ErrMessageBadPacket
		}
		msgID = readUint16(data, &bookmark)
	}

	return &Publish{
		Header:    hdr,
		Topic:     topic,
		Payload:   data[bookmark:],
		MessageID: msgID,
	}, nil
}

func decodePuback(data []byte) Message {
	bookmark := uint32(0)
	msgID := readUint16(data, &bookmark)
	return &Puback{
		MessageID: msgID,
	}
}

func decodePubrec(data []byte) Message {
	bookmark := uint32(0)
	msgID := readUint16(data, &bookmark)
	return &Pubrec{
		MessageID: msgID,
	}
}

func decodePubrel(data []byte, hdr Header) Message {
	bookmark := uint32(0)
	msgID := readUint16(data, &bookmark)
	return &Pubrel{
		Header:    hdr,
		MessageID: msgID,
	}
}

func decodePubcomp(data []byte) Message {
	bookmark := uint32(0)
	msgID := readUint16(data, &bookmark)
	return &Pubcomp{
		MessageID: msgID,
	}
}

func decodeSubscribe(data []byte, hdr Header) (Message, error) {
	bookmark := uint32(0)
	msgID := readUint16(data, &bookmark)
	var topics []TopicQOSTuple
	maxlen := uint32(len(data))
	var err error
	for bookmark < maxlen {
		var t TopicQOSTuple
		t.Topic, err = readString(data, &bookmark)
		if err != nil {
			return nil, err
		}
		if bookmark >= maxlen {
			return nil, ErrMessageBadPacket
		}
		qos := data[bookmark]
		bookmark++
		t.Qos = uint8(qos)
		topics = append(topics, t)
	}
	return &Subscribe{
		Header:        hdr,
		MessageID:     msgID,
		Subscriptions: topics,
	}, nil
}

func decodeSuback(data []byte) Message {
	bookmark := uint32(0)
	msgID := readUint16(data, &bookmark)
	var qoses []uint8
	maxlen := uint32(len(data))
	for bookmark < maxlen {
		qos := data[bookmark]
		bookmark++
		qoses = append(qoses, qos)
	}
	return &Suback{
		MessageID: msgID,
		Qos:       qoses,
	}
}

func decodeUnsubscribe(data []byte, hdr Header) (Message, error) {
	bookmark := uint32(0)
	var topics []TopicQOSTuple
	msgID := readUint16(data, &bookmark)
	maxlen := uint32(len(data))
	var err error
	for bookmark < maxlen {
		var t TopicQOSTuple
		t.Topic, err = readString(data, &bookmark)
		if err != nil {
			return nil, err
		}
		topics = append(topics, t)
	}
	return &Unsubscribe{
		Header:    hdr,
		MessageID: msgID,
		Topics:    topics,
	}, nil
}

func decodeUnsuback(data []byte) Message {
	bookmark := uint32(0)
	msgID := readUint16(data, &bookmark)
	return &Unsuback{
		MessageID: msgID,
	}
}

func writeHeader(buf []byte, msgType uint8, h *Header, length int) int {
	var firstByte byte
	firstByte |= msgType << 4
	if h != nil {
		firstByte |= boolToUInt8(h.DUP) << 3
		firstByte |= h.QOS << 1
		firstByte |= boolToUInt8(h.Retain)
	}

	numBytes, bitField := encodeLength(uint32(length))
	offset := maxHeaderSize - int(numBytes) - 1

	buf[offset] = byte(firstByte)
	for i := offset + 1; i < maxHeaderSize; i++ {
		buf[i] = byte(bitField >> ((numBytes - 1) * 8))
		numBytes--
	}

	return int(offset)
}

func writeString(buf, v []byte) int {
	length := len(v)
	offset := writeUint16(buf, uint16(length))
	copy(buf[offset:], v)
	return offset + length
}

func writeUint16(buf []byte, v uint16) int {
	buf[0] = byte((v & 0xFF00) >> 8)
	buf[1] = byte(v & 0x00FF)
	return 2
}

func writeUint8(buf []byte, v uint8) int {
	buf[0] = v
	return 1
}

func readString(b []byte, startsAt *uint32) ([]byte, error) {
	if *startsAt+1 >= uint32(len(b)) {
		return nil, ErrMessageBadPacket
	}
	l := readUint16(b, startsAt)
	if uint32(l)+*startsAt > uint32(len(b)) {
		return nil, ErrMessageBadPacket
	}
	v := b[*startsAt : uint32(l)+*startsAt]
	*startsAt += uint32(l)
	return v, nil
}

func readUint16(b []byte, startsAt *uint32) uint16 {
	b0 := uint16(b[*startsAt])
	b1 := uint16(b[*startsAt+1])
	*startsAt += 2

	return (b0 << 8) + b1
}

func boolToUInt8(v bool) uint8 {
	if v {
		return 0x1
	}
	return 0x0
}

func encodeLength(bodyLength uint32) (uint8, uint32) {
	if bodyLength == 0 {
		return 1, 0
	}

	var bitField uint32
	var numBytes uint8
	var encodedBytes []byte

	for {
		digit := byte(bodyLength % 128)
		bodyLength /= 128
		if bodyLength > 0 {
			digit |= 0x80
		}
		encodedBytes = append(encodedBytes, digit)
		numBytes++
		if bodyLength == 0 {
			break
		}
	}

	for i := len(encodedBytes) - 1; i >= 0; i-- {
		bitField <<= 8
		bitField |= uint32(encodedBytes[i])
	}

	return numBytes, bitField
}

type Client struct {
	ID           string
	Conn         net.Conn
	Writer       *bufio.Writer
	Reader       *bufio.Reader
	IsConnected  bool
	Outgoing     chan *Publish
	Done         chan struct{}
	CleanSession bool
}

type Broker struct {
	clients       map[string]*Client
	subscriptions map[string]map[string]*Client
	mu            sync.RWMutex
}

func NewBroker() *Broker {
	return &Broker{
		clients:       make(map[string]*Client),
		subscriptions: make(map[string]map[string]*Client),
	}
}

func (b *Broker) HandleConnection(conn net.Conn) {
	log.Printf("Новое подключение от: %s", conn.RemoteAddr())

	client := &Client{
		Conn:        conn,
		Writer:      bufio.NewWriter(conn),
		Reader:      bufio.NewReader(conn),
		IsConnected: false,
		Outgoing:    make(chan *Publish, 100),
		Done:        make(chan struct{}),
	}

	go b.readLoop(client)
	go b.writeLoop(client)
}

func (b *Broker) readLoop(client *Client) {
	defer func() {
		log.Printf("Клиент %s отключился", client.ID)
		b.removeClient(client)
		client.Conn.Close()
		close(client.Done)
	}()

	for {
		msg, err := DecodePacket(client.Reader, int64(MaxMessageSize))
		if err != nil {
			if err == io.EOF {
				log.Printf("Соединение закрыто клиентом %s", client.ID)
				return
			}
			log.Printf("Ошибка декодирования пакета от %s: %v", client.ID, err)
			return
		}

		log.Printf("Получен пакет %T от клиента %s", msg, client.ID)

		switch packet := msg.(type) {
		case *Connect:
			b.handleConnect(client, packet)
		case *Publish:
			b.handlePublish(client, packet)
		case *Subscribe:
			b.handleSubscribe(client, packet)
		case *Unsubscribe:
			b.handleUnsubscribe(client, packet)
		case *Disconnect:
			log.Printf("Клиент %s отправил DISCONNECT", client.ID)
			return
		case *Pingreq:
			pingresp := &Pingresp{}
			_, err := pingresp.EncodeTo(client.Writer)
			if err != nil {
				log.Printf("Ошибка отправки PINGRESP клиенту %s: %v", client.ID, err)
				return
			}
			client.Writer.Flush()
		default:
			log.Printf("Неизвестный или неподдерживаемый тип пакета %T от %s", packet, client.ID)
		}
	}
}

func (b *Broker) writeLoop(client *Client) {
	for {
		select {
		case msg := <-client.Outgoing:
			_, err := msg.EncodeTo(client.Writer)
			if err != nil {
				log.Printf("Ошибка отправки сообщения клиенту %s: %v", client.ID, err)
				return
			}
			err = client.Writer.Flush()
			if err != nil {
				log.Printf("Ошибка сброса буфера для клиента %s: %v", client.ID, err)
				return
			}
		case <-client.Done:
			return
		}
	}
}

func (b *Broker) handleConnect(client *Client, packet *Connect) {
	client.ID = string(packet.ClientID)
	client.IsConnected = true
	client.CleanSession = packet.CleanSeshFlag

	log.Printf("Клиент %s подключился (ClientID: %s)", client.Conn.RemoteAddr(), client.ID)

	b.mu.Lock()
	b.clients[client.ID] = client
	b.mu.Unlock()

	connack := &Connack{ReturnCode: 0x00}
	_, err := connack.EncodeTo(client.Writer)
	if err != nil {
		log.Printf("Ошибка отправки CONNACK клиенту %s: %v", client.ID, err)
		client.Conn.Close()
		return
	}
	client.Writer.Flush()

	if !client.CleanSession {
		log.Printf("Клиент %s запросил CleanSession=false, но в этой версии брокера состояние не сохраняется.", client.ID)
	}
}

func (b *Broker) handlePublish(publisher *Client, packet *Publish) {
	topic := string(packet.Topic)
	payload := string(packet.Payload)
	log.Printf("Получено PUBLISH от %s: Тема='%s', Сообщение='%s'", publisher.ID, topic, payload)

	b.mu.RLock()
	defer b.mu.RUnlock()

	if subscribers, ok := b.subscriptions[topic]; ok {
		for clientID, subscriber := range subscribers {
			if clientID == publisher.ID {
				continue
			}
			log.Printf("Отправка сообщения по теме '%s' подписчику %s", topic, subscriber.ID)
			pubToSend := &Publish{
				Header:    packet.Header,
				Topic:     packet.Topic,
				MessageID: packet.MessageID,
				Payload:   packet.Payload,
			}
			select {
			case subscriber.Outgoing <- pubToSend:
			case <-time.After(1 * time.Second):
				log.Printf("Таймаут при отправке сообщения клиенту %s, канал заблокирован.", subscriber.ID)
			}
		}
	}
}

func (b *Broker) handleSubscribe(client *Client, packet *Subscribe) {
	log.Printf("Клиент %s отправил SUBSCRIBE (MessageID: %d)", client.ID, packet.MessageID)

	qosResponses := make([]uint8, len(packet.Subscriptions))

	b.mu.Lock()
	defer b.mu.Unlock()

	for i, sub := range packet.Subscriptions {
		topic := string(sub.Topic)
		requestedQoS := sub.Qos

		grantedQoS := uint8(0x00)
		qosResponses[i] = grantedQoS

		if _, ok := b.subscriptions[topic]; !ok {
			b.subscriptions[topic] = make(map[string]*Client)
		}
		b.subscriptions[topic][client.ID] = client
		log.Printf("Клиент %s подписался на тему '%s' с запрошенным QoS %d, предоставлен QoS %d",
			client.ID, topic, requestedQoS, grantedQoS)
	}

	suback := &Suback{
		MessageID: packet.MessageID,
		Qos:       qosResponses,
	}
	_, err := suback.EncodeTo(client.Writer)
	if err != nil {
		log.Printf("Ошибка отправки SUBACK клиенту %s: %v", client.ID, err)
		client.Conn.Close()
		return
	}
	client.Writer.Flush()
}

func (b *Broker) handleUnsubscribe(client *Client, packet *Unsubscribe) {
	log.Printf("Клиент %s отправил UNSUBSCRIBE (MessageID: %d)", client.ID, packet.MessageID)

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, topicTuple := range packet.Topics {
		topic := string(topicTuple.Topic)
		if subscribers, ok := b.subscriptions[topic]; ok {
			if _, clientSubscribed := subscribers[client.ID]; clientSubscribed {
				delete(subscribers, client.ID)
				if len(subscribers) == 0 {
					delete(b.subscriptions, topic)
				}
				log.Printf("Клиент %s отписался от темы '%s'", client.ID, topic)
			} else {
				log.Printf("Клиент %s пытался отписаться от темы '%s', на которую он не был подписан", client.ID, topic)
			}
		} else {
			log.Printf("Клиент %s пытался отписаться от несуществующей темы '%s'", client.ID, topic)
		}
	}

	unsuback := &Unsuback{
		MessageID: packet.MessageID,
	}
	_, err := unsuback.EncodeTo(client.Writer)
	if err != nil {
		log.Printf("Ошибка отправки UNSUBACK клиенту %s: %v", client.ID, err)
		client.Conn.Close()
		return
	}
	client.Writer.Flush()
}

func (b *Broker) removeClient(client *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if client.ID != "" {
		delete(b.clients, client.ID)
		for topic, subscribers := range b.subscriptions {
			if _, ok := subscribers[client.ID]; ok {
				delete(subscribers, client.ID)
				if len(subscribers) == 0 {
					delete(b.subscriptions, topic)
				}
			}
		}
	}
}
