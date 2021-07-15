package eip

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"strconv"
	"net"
	"time"
	
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/inputs"
)

type PLC struct {
	TagsToRead []string `toml:"TagsToRead"`
	IPAddress string `toml:"IPAddress"`
	ProcessorSlot byte `toml:"ProcessorSlot"`
	Micro800 bool
	Port uint16
	VendorID uint16
	Context uint64
	ContextPointer uint32
	Socket net.Conn
	SocketConnected bool
	OTNetworkConnectionID uint32
	SessionHandle uint32
	SessionRegistered bool
	SerialNumber uint16
	OriginatorSerialNumber uint16
	SequenceCounter uint16
	Offset uint16
	KnownTags map[string]TagMap
	TagList []LGXTag
	ProgramNames []string
	StructIdentifier uint16
	CIPTypes map[byte]CIPTypesStruct
}

var PLCConfig = `
  ## Set the amplitude
  TagsToRead = ["tag1",
	"tag2",
	"tag3"]
  IPAddress = "192.168.14.169"
  ProcessSlot = 3
`

func (plc *PLC) SampleConfig() string {
	return PLCConfig
}

func (plc *PLC) Description() string {
	return "Requests data from PLCs for configured tags at configured interval."
}

func (plc *PLC) Gather(acc telegraf.Accumulator) error {
	values := plc.MultiRead(plc.TagsToRead)

	fields := make(map[string]interface{})
	tags := make(map[string]string)
	
	for n, t := range plc.TagsToRead {
		fields["value"] = values[n]
		tags["TagName"] = t
		acc.AddFields("eip", fields, tags)
	}

	return nil
}

func init() {
	inputs.Add("eip", func() telegraf.Input { return &PLC{} })
}


var context_dict = map[uint32]uint64{
	0: 0x6572276557,
	1: 0x6f6e,
	2: 0x676e61727473,
	3: 0x737265,
	4: 0x6f74,
	5: 0x65766f6c,
	6: 0x756f59,
	7: 0x776f6e6b,
	8: 0x656874,
	9: 0x73656c7572,
	10: 0x646e61,
	11: 0x6f73,
	12: 0x6f64,
	13: 0x49,
	14: 0x41,
	15: 0x6c6c7566,
	16: 0x74696d6d6f63,
	17: 0x7327746e656d,
	18: 0x74616877,
	19: 0x6d2749,
	20: 0x6b6e696874,
	21: 0x676e69,
	22: 0x666f,
	23: 0x756f59,
	24: 0x746e646c756f77,
	25: 0x746567,
	26: 0x73696874,
	27: 0x6d6f7266,
	28: 0x796e61,
	29: 0x726568746f,
	30: 0x797567,
	31: 0x49,
	32: 0x7473756a,
	33: 0x616e6e6177,
	34: 0x6c6c6574,
	35: 0x756f79,
	36: 0x776f68,
	37: 0x6d2749,
	38: 0x676e696c656566,
	39: 0x6174746f47,
	40: 0x656b616d,
	41: 0x756f79,
	42: 0x7265646e75,
	43: 0x646e617473,
	44: 0x726576654e,
	45: 0x616e6e6f67,
	46: 0x65766967,
	47: 0x756f79,
	48: 0x7075,
	49: 0x726576654e,
	50: 0x616e6e6f67,
	51: 0x74656c,
	52: 0x756f79,
	53: 0x6e776f64,
	54: 0x726576654e,
	55: 0x616e6e6f67,
	56: 0x6e7572,
	57: 0x646e756f7261,
	58: 0x646e61,
	59: 0x747265736564,
	60: 0x756f79,
	61: 0x726576654e,
	62: 0x616e6e6f67,
	63: 0x656b616d,
	64: 0x756f79,
	65: 0x797263,
	66: 0x726576654e,
	67: 0x616e6e6f67,
	68: 0x796173,
	69: 0x657962646f6f67,
	70: 0x726576654e,
	71: 0x616e6e6f67,
	72: 0x6c6c6574,
	73: 0x61,
	74: 0x65696c,
	75: 0x646e61,
	76: 0x74727568,
	77: 0x756f79,
	78: 0x6576276557,
	79: 0x6e776f6e6b,
	80: 0x68636165,
	81: 0x726568746f,
	82: 0x726f66,
	83: 0x6f73,
	84: 0x676e6f6c,
	85: 0x72756f59,
	86: 0x73277472616568,
	87: 0x6e656562,
	88: 0x676e69686361,
	89: 0x747562,
	90: 0x657227756f59,
	91: 0x6f6f74,
	92: 0x796873,
	93: 0x6f74,
	94: 0x796173,
	95: 0x7469,
	96: 0x656469736e49,
	97: 0x6577,
	98: 0x68746f62,
	99: 0x776f6e6b,
	100: 0x732774616877,
	101: 0x6e656562,
	102: 0x676e696f67,
	103: 0x6e6f,
	104: 0x6557,
	105: 0x776f6e6b,
	106: 0x656874,
	107: 0x656d6167,
	108: 0x646e61,
	109: 0x6572276577,
	110: 0x616e6e6f67,
	111: 0x79616c70,
	112: 0x7469,
	113: 0x646e41,
	114: 0x6669,
	115: 0x756f79,
	116: 0x6b7361,
	117: 0x656d,
	118: 0x776f68,
	119: 0x6d2749,
	120: 0x676e696c656566,
	121: 0x74276e6f44,
	122: 0x6c6c6574,
	123: 0x656d,
	124: 0x657227756f79,
	125: 0x6f6f74,
	126: 0x646e696c62,
	127: 0x6f74,
	128: 0x656573,
	129: 0x726576654e,
	130: 0x616e6e6f67,
	131: 0x65766967,
	132: 0x756f79,
	133: 0x7075,
	134: 0x726576654e,
	135: 0x616e6e6f67,
	136: 0x74656c,
	137: 0x756f79,
	138: 0x6e776f64,
	139: 0x726576654e,
	140: 0x6e7572,
	141: 0x646e756f7261,
	142: 0x646e61,
	143: 0x747265736564,
	144: 0x756f79,
	145: 0x726576654e,
	146: 0x616e6e6f67,
	147: 0x656b616d,
	148: 0x756f79,
	149: 0x797263,
	150: 0x726576654e,
	151: 0x616e6e6f67,
	152: 0x796173,
	153: 0x657962646f6f67,
	154: 0x726576654e,
	155: 0xa680e2616e6e6f67,
}

var cipErrorCodes = map[uint16]string{
	0x00: "Success",
	 0x01: "Connection failure",
	 0x02: "Resource unavailable",
	 0x03: "Invalid parameter value",
	 0x04: "Path segment error",
	 0x05: "Path destination unknown",
	 0x06: "Partial transfer",
	 0x07: "Connection lost",
	 0x08: "Service not supported",
	 0x09: "Invalid Attribute",
	 0x0A: "Attribute list error",
	 0x0B: "Already in requested mode/state",
	 0x0C: "Object state conflict",
	 0x0D: "Object already exists",
	 0x0E: "Attribute not settable",
	 0x0F: "Privilege violation",
	 0x10: "Device state conflict",
	 0x11: "Reply data too large",
	 0x12: "Fragmentation of a premitive value",
	 0x13: "Not enough data",
	 0x14: "Attribute not supported",
	 0x15: "Too much data",
	 0x16: "Object does not exist",
	 0x17: "Service fragmentation sequence not in progress",
	 0x18: "No stored attribute data",
	 0x19: "Store operation failure",
	 0x1A: "Routing failure, request packet too large",
	 0x1B: "Routing failure, response packet too large",
	 0x1C: "Missing attribute list entry data",
	 0x1D: "Invalid attribute value list",
	 0x1E: "Embedded service error",
	 0x1F: "Vendor specific",
	 0x20: "Invalid Parameter",
	 0x21: "Write once value or medium already written",
	 0x22: "Invalid reply received",
	 0x23: "Buffer overflow",
	 0x24: "Invalid message format",
	 0x25: "Key failure in path",
	 0x26: "Path size invalid",
	 0x27: "Unexpected attribute in list",
	 0x28: "Invalid member ID",
	 0x29: "Member not settable",
	 0x2A: "Group 2 only server general failure",
	 0x2B: "Unknown Modbus error",
	 0x2C: "Attribute not gettable",
}

type TagMap struct {
	dataType byte
	dataLen int
}
type CIPTypesStruct struct {
	dataLen int
	dataType string
	format rune
}


type LGXTag struct {
	InstanceID uint32
	DataType byte
	BitPosition byte
	ArrayDims byte
	IsStruct bool
	IsSystem bool
	TagName string
}

type RegSession struct {
	EIPCommand uint16 //#(H)Register Session Command   (Vol 2 2-3.2)
	EIPLength uint16 //#(H)Lenght of Payload		  (2-3.3)
	EIPSessionHandle uint32 //#(I)Session Handle			 (2-3.4)
	EIPStatus uint32 //#(I)Status always 0x00		 (2-3.5)
	EIPContext uint64 //#(Q)						   (2-3.6)
	EIPOptions uint32 //#(I)Options always 0x00		(2-3.7)
		//#Begin Command Specific Data
	EIPProtocolVersion uint16 //#(H)Always 0x01				(2-4.7)
	EIPOptionFlag uint16 //#(H)Always 0x00				(2-4.7)
}

type UnregSession struct {
	EIPCommand uint16 //#(H)Register Session Command   (Vol 2 2-3.2)
	EIPLength uint16 //#(H)Lenght of Payload		  (2-3.3)
	EIPSessionHandle uint32 //#(I)Session Handle			 (2-3.4)
	EIPStatus uint32 //#(I)Status always 0x00		 (2-3.5)
	EIPContext uint64 //#(Q)						   (2-3.6)
	EIPOptions uint32 //#(I)Options always 0x00		(2-3.7)
}

type CIPForwardOpen struct {
   CIPService byte
   CIPPathSize byte
   CIPClassType byte
   CIPClass byte
   CIPInstanceType byte
   CIPInstance byte
   CIPPriority byte
   CIPTimeoutTicks byte
   CIPOTConnectionID uint32
   CIPTOConnectionID uint32
   CIPConnectionSerialNumber uint16
   CIPVendorID uint16
   CIPOriginatorSerialNumber uint32
   CIPMultiplier uint32
   CIPOTRPI uint32
   CIPOTNetworkConnectionParameters int16
   CIPTORPI uint32
   CIPTONetworkConnectionParameters int16
   CIPTransportTrigger byte
}

type CIPForwardClose struct {
   CIPService byte
   CIPPathSize byte
   CIPClassType byte
   CIPClass byte
   CIPInstanceType byte
   CIPInstance byte
   CIPPriority byte
   CIPTimeoutTicks byte
   CIPConnectionSerialNumber uint16
   CIPVendorID uint16
   CIPOriginatorSerialNumber uint32
}

type EIPSendRRDataHeader struct {
//'<HHIIQIIHHHHHH',
	EIPCommand uint16
	EIPLength uint16
	EIPSessionHandle uint32
	EIPStatus uint32
	EIPContext uint64
	EIPOptions uint32
	EIPInterfaceHandle uint32
	EIPTimeout uint16
	EIPItemCount uint16
	EIPItem1Type uint16
	EIPItem1Length uint16
	EIPItem2Type uint16
	EIPItem2Length uint16
}



type MultiServiceHeader struct {
	MultiService byte
	MultiPathSize byte
	MutliClassType byte
	MultiClassSegment byte
	MultiInstanceType byte
	MultiInstanceSegment byte
}

type EIPHeader struct {
	EIPCommand uint16
	EIPLength uint16
	EIPSessionHandle uint32
	EIPStatus uint32
	EIPContext uint64
	EIPOptions uint32
	EIPInterfaceHandle uint32
	EIPTimeout uint16
	EIPItemCount uint16
	EIPItem1ID uint16
	EIPItem1Length uint16
	EIPItem1 uint32
	EIPItem2ID uint16
	EIPItem2Length uint16
	EIPSequence uint16
}

type Attribute struct {
	AttributeService byte
	AttributeSize byte
	AttributeClassType byte
	AttributeClass byte
	AttributeInstanceType byte
	AttributeInstance byte
	AttributeCount uint16
	TimeAttribute uint16
}

func (plc *PLC)Init() {
	plc.IPAddress = "192.168.14.169"
	plc.ProcessorSlot = 3
	plc.Micro800 = false
	plc.Port = 44818
	plc.VendorID = 0x1337
	plc.Context = 0x00
	plc.ContextPointer = 0
	//plc.Socket = socket.socket()
	//plc.Socket.settimeout(0.5)
	plc.SocketConnected = false
	//plc.OTNetworkConnectionID=nil
	plc.SessionHandle = 0x0000
	plc.SessionRegistered = false
	plc.SerialNumber = uint16(rand.Intn(65000))
	plc.OriginatorSerialNumber = 42
	plc.SequenceCounter = 1
	plc.Offset = 0
	plc.KnownTags = make(map[string]TagMap)
	plc.StructIdentifier = 0x0fCE
	plc.CIPTypes = make(map[byte]CIPTypesStruct)
	plc.CIPTypes[160] = CIPTypesStruct{dataLen: 0, dataType: "STRUCT", format: 'B'}
	plc.CIPTypes[193] = CIPTypesStruct{dataLen: 1, dataType: "BOOL", format: '?'}
	plc.CIPTypes[194] = CIPTypesStruct{dataLen: 1, dataType: "SINT", format: 'b'}
	plc.CIPTypes[195] = CIPTypesStruct{dataLen: 2, dataType: "INT", format: 'h'}
	plc.CIPTypes[196] = CIPTypesStruct{dataLen: 4, dataType: "DINT", format: 'i'}
	plc.CIPTypes[197] = CIPTypesStruct{dataLen: 8, dataType: "LINT", format: 'q'}
	plc.CIPTypes[198] = CIPTypesStruct{dataLen: 1, dataType: "USINT", format: 'B'}
	plc.CIPTypes[199] = CIPTypesStruct{dataLen: 2, dataType: "UINT", format: 'H'}
	plc.CIPTypes[200] = CIPTypesStruct{dataLen: 4, dataType: "UDINT", format: 'I'}
	plc.CIPTypes[201] = CIPTypesStruct{dataLen: 8, dataType: "LWORD", format: 'Q'}
	plc.CIPTypes[202] = CIPTypesStruct{dataLen: 4, dataType: "REAL", format: 'f'}
	plc.CIPTypes[203] = CIPTypesStruct{dataLen: 8, dataType: "LREAL", format: 'd'}
	plc.CIPTypes[211] = CIPTypesStruct{dataLen: 4, dataType: "DWORD", format: 'I'}
	plc.CIPTypes[218] = CIPTypesStruct{dataLen: 0, dataType: "STRING", format: 'B'}

}

func (plc *PLC)_readTag(tag string, elements uint16) []interface{} {
	var result []interface{}
	plc.Offset = 0
	
	if !plc._connect(){
		return nil
	}
	
	var tagData []byte
	var readRequest []byte
	var eipHeader []byte
	var status uint16
	var err string

	t,b,i := _tagNameParser(tag, 0)
	plc._initialRead(t, b)

	datatype := plc.KnownTags[b].dataType
	bitCount := plc.CIPTypes[datatype].dataLen * 8
	
	if datatype == 211 {
		//# bool array
		tagData = plc._buildTagIOI(tag, true)
		words := _getWordCount(uint32(i), elements, bitCount)
		readRequest = plc._addReadIOI(tagData, words)
	} else if BitofWord(t) {
		//# bits of word
		split_tag := strings.Split(tag, ".")
		bitPos, _ := strconv.Atoi(split_tag[len(split_tag)-1])

		tagData = plc._buildTagIOI(tag, false)
		words := _getWordCount(uint32(bitPos), elements, bitCount)

		readRequest = plc._addReadIOI(tagData, words)
	} else {
		//# everything else
		tagData = plc._buildTagIOI(tag, false)
		readRequest = plc._addReadIOI(tagData, elements)
	}
	

	eipHeader = plc._buildEIPHeader(len(readRequest))
	retData := plc._getBytes(append(eipHeader, readRequest...))

	if len(retData)>= 48 {
		status = binary.LittleEndian.Uint16(retData[48:])
	} else {
		status = 1
	}
	//fmt.Printf("%x\n", retData)
	if status == 0 || status == 6 {
		result = plc._parseReply(tag, elements, retData)
	} else {
		if code, ok := cipErrorCodes[status]; ok {
			err = code
		} else {
			err = "Unknown error"
		}
		result = append(result, err)
	}
	return result
	
}

func (plc *PLC)_multiRead(args []string) []interface{} {
	/*
	Processes the multiple read request
	*/
	var result []interface{}
	var serviceSegments [][]byte
	var segments []byte
	var status uint16
	var packetSize int
	var offset int
	tagCount := len(args)

	if !plc._connect() {
		return nil
	}
	offsets := new(bytes.Buffer)
	readRequest := new(bytes.Buffer)

	for i:=0; i<tagCount; i++ {
		t,_,_ := _tagNameParser(args[i], 0)
		//plc._initialRead(t, b)

		tagIOI := plc._buildTagIOI(t, false)
		readIOI := plc._addReadIOI(tagIOI, 1)
		serviceSegments = append(serviceSegments, readIOI)
	}
	

	multiHeader := plc._buildMultiServiceHeader()
	eipHeader := plc._buildEIPHeader(len(multiHeader)) //just for size calc

	currentCount := 0
	for totalCount := 0; totalCount<tagCount; currentCount=0 {
		offsets.Reset()
		readRequest.Reset()
		segments = nil
		packetSize = len(eipHeader)+len(multiHeader)+2
		
		for i:=totalCount; i<tagCount; i++ { //512 bytes max packet (256 words)
			packetSize += 2 + len(serviceSegments[i])
			if packetSize < 512 {
				segments = append(segments, serviceSegments[i]...)
				currentCount++
			} else { //packet too large, need to stop
				break
			}
		}

		offset = 2+2*currentCount //2 bytes for service count + 2 per offset value 
		for i:= 0; i < currentCount; i++ {
			binary.Write(offsets, binary.LittleEndian, uint16(offset))
			offset += len(serviceSegments[i+totalCount]) //in bytes
		}
		totalCount += currentCount
		
		binary.Write(readRequest, binary.LittleEndian, multiHeader)
		binary.Write(readRequest, binary.LittleEndian, uint16(currentCount))
		binary.Write(readRequest, binary.LittleEndian, offsets.Bytes())
		binary.Write(readRequest, binary.LittleEndian, segments)

		eipHeader = plc._buildEIPHeader(readRequest.Len())
		
		retData := plc._getBytes(append(eipHeader, readRequest.Bytes()...))
		
		if retData != nil && len(retData)>48 {
			status = binary.LittleEndian.Uint16(retData[48:])
		} else {
			status = 0x01
		}

		if status == 0 {
			result = append(result, plc._multiParser(retData)...)
		} else {
			var err string
			if code, ok := cipErrorCodes[status]; ok {
				err = code
			} else {
				err = "Unknown error"
			}
			result = append(result, "Multi-read failed: " + err)
		}
	}
	
	return result
}

func (plc *PLC)_getPLCTime() time.Time {
	/*
	Requests the PLC clock time
	*/ 
	if !plc._connect() {
		return time.Time{} //can't return nil for time.Time
	}
	ap := Attribute {
		AttributeService: 0x03,
		AttributeSize: 0x02,
		AttributeClassType: 0x20,
		AttributeClass: 0x8B,
		AttributeInstanceType: 0x24,
		AttributeInstance: 0x01,
		AttributeCount: 0x01,
		TimeAttribute: 0x0B,
	}
	buf := new(bytes.Buffer)	
	if err := binary.Write(buf, binary.LittleEndian, ap); err != nil {
		fmt.Println(err)
		return time.Time{}
	} 

	eipHeader := plc._buildEIPHeader(buf.Len())
	request := append(eipHeader, buf.Bytes()...)
	retData := plc._getBytes(request)
	
	var status uint16
	if len(retData) >= 48 {
		status = binary.LittleEndian.Uint16(retData[48:])
	} else {
		status = 0x01
	}

	if status == 0 {
		//# get the time from the packet
		plcTime := binary.LittleEndian.Uint64(retData[56:])
		humanTime := time.Unix(0, int64(plcTime)*1000)
		return humanTime
	} else {
		var err string
		if code, ok := cipErrorCodes[status]; ok {
			err = code
		} else {
			err = "Unknown error"
		}
		fmt.Println("Failed to get PLC time: " + err)
		return time.Time{}
	}
}

func (plc *PLC)_getTagList() []LGXTag {
	/*
	Requests the controller tag list and returns a list of LgxTag type
	Also updates the internal list of LGXTag (plc.TagList)
	*/
	if !plc._connect() {
		return nil
	}
	var status uint16
	plc.Offset = 0
	plc.TagList = nil
	plc.ProgramNames = nil
	
	request := plc._buildTagListRequest("")
	eipHeader := plc._buildEIPHeader(len(request))
	retData := plc._getBytes(append(eipHeader, request...))
	if len(retData)>48 {
		status = binary.LittleEndian.Uint16(retData[48:])
		plc._extractTagPacket(retData, "")
	} else {
		status = uint16(0x01)
	}
	if status != 0 && status != 6 {
		var err string
		if code, ok := cipErrorCodes[status]; ok {
			err = code
		} else {
			err = "Unknown error"
		}
		fmt.Println("Error while getting taglist: " + err)
	}

	for status == 6 {
		plc.Offset += 1
		request = plc._buildTagListRequest("")
		eipHeader = plc._buildEIPHeader(len(request))
		retData = plc._getBytes(append(eipHeader, request...))
		plc._extractTagPacket(retData, "")
		status = binary.LittleEndian.Uint16(retData[48:])
		//#time.sleep(0.25)
	}

	/*
	When we're done with the controller scoped tags,
	request the program scoped tags
	*/
	for _, programName := range plc.ProgramNames {

		plc.Offset = 0

		request = plc._buildTagListRequest(programName)
		eipHeader = plc._buildEIPHeader(len(request))
		retData = plc._getBytes(append(eipHeader, request...))
		if len(retData)>48 {
			status = binary.LittleEndian.Uint16(retData[48:])
			plc._extractTagPacket(retData, programName)
		} else {
			status = uint16(0x01)
		}
		if status != 0 && status != 6 {
			var err string
			if code, ok := cipErrorCodes[status]; ok {
				err = code
			} else {
				err = "Unknown error"
			}
			fmt.Println("Error while getting taglist: " + err)
		}

		for status == 6 {
			plc.Offset += 1
			request = plc._buildTagListRequest(programName)
			eipHeader = plc._buildEIPHeader(len(request))
			retData = plc._getBytes(append(eipHeader, request...))
			plc._extractTagPacket(retData, programName)
			status = binary.LittleEndian.Uint16(retData[48:])
			//#time.sleep(0.25)
		}
	}

	return plc.TagList
}

func (plc *PLC)_buildTagListRequest(programName string) []byte {
	/*
	Build the request for the PLC tags
	Program scoped tags will pass the program name for the request
	*/
	var TagListRequest []byte
	PathSegment := new(bytes.Buffer)

	//If we're dealing with program scoped tags...
	if len(programName) > 0 {
		PathSegment.WriteByte(0x91)
		PathSegment.WriteByte(byte(len(programName)))
		PathSegment.WriteString(programName)
	}
	//# if odd number of characters, need to add a byte to the end.
	if len(programName) % 2 > 0 {
		PathSegment.WriteByte(0x00)
	}
	
	binary.Write(PathSegment, binary.LittleEndian, uint16(0x6B20))

	if plc.Offset < 256 {
		PathSegment.WriteByte(0x24)
		PathSegment.WriteByte(byte(plc.Offset))
	} else {
		binary.Write(PathSegment, binary.LittleEndian, uint16(0x25))
		binary.Write(PathSegment, binary.LittleEndian, plc.Offset)
	}
	
	Service := byte(0x55)
	PathSegmentLen := PathSegment.Len() / 2

	AttributeCount := uint16(0x03)
	SymbolType := uint16(0x02)
	ByteCount := uint16(0x07)
	SymbolName := uint16(0x01)

	TagListRequest = append(TagListRequest, Service)
	TagListRequest = append(TagListRequest, byte(PathSegmentLen))
	
	TagListRequest = append(TagListRequest, PathSegment.Bytes()...)
	
	tmp := make([]byte, 8)
	binary.LittleEndian.PutUint16(tmp[0:], AttributeCount)
	binary.LittleEndian.PutUint16(tmp[2:], SymbolType)
	binary.LittleEndian.PutUint16(tmp[4:], ByteCount)
	binary.LittleEndian.PutUint16(tmp[6:], SymbolName)
	TagListRequest = append(TagListRequest, tmp...)

	return TagListRequest
}

func (plc *PLC)_extractTagPacket(data []byte, programName string) {
	// the first tag in a packet starts at byte 50
	packetStart := uint(50)
	var tagLen uint16
	var packet []byte
	var tag LGXTag

	for packetStart < uint(len(data)) {
		// get the length of the tag name
		tagLen = binary.LittleEndian.Uint16(data[packetStart+8:])
		if tagLen == 0 {
			break
		}
		// get a single tag from the packet
		packet = data[packetStart:packetStart+uint(tagLen)+10]
		// extract the offset
		plc.Offset = binary.LittleEndian.Uint16(packet[0:])
		// add the tag to our tag list
		tag = plc._parseLgxTag(packet, programName)
		// filter out garbage
		//if _, ok := plc.CIPTypes[tag.DataType]; ok && !strings.Contains(tag.TagName, "__DEFVAL_") && !strings.Contains(tag.TagName, "Routine:") {
		if !tag.IsSystem && !strings.HasPrefix(tag.TagName, "__") { //would that cover everything?
			plc.TagList = append(plc.TagList, tag)
		}
		if len(programName) == 0 {
			if strings.Contains(tag.TagName, "Program:") {
				plc.ProgramNames = append(plc.ProgramNames, tag.TagName)
			}
		}
		// increment ot the next tag in the packet
		packetStart = packetStart+uint(tagLen)+10
	}
}

func (plc *PLC)_parseLgxTag(packet []byte, programName string) LGXTag {
	var tag LGXTag
/*Sample Data
InstanceID DataType ByteCount? Length TagName
10050000 30 81 0c00 0e00 424154315f4c5144355f46434e53 
11050000 1e 89 e81a 1800 424154315f4c5144355f4d41535445525f464f524d554c41
12050000 7b 82 5000 1000 424154315f4c5144355f504152414d53 
13050000 e8 8c 1c00 1000 424154315f4c5144355f535441545553 
14050000 ca 00 0400 1a00 424154315f4c5144355f544f54414c5f5441524745545f574754 
15050000 c1 00 0100 1700 424154315f4d41535445525f52554e5f50524553454e54
16050000 e9 82 5807 1800 424154315f4d41535445525f5343414c455f484541444552
17050000 e9 82 5807 1700 424154315f4d4943524f5f43484153455f484541444552
18050000 c1 00 0100 1400 424154315f4d4943524f5f43484153455f534554
19050000 c1 00 0100 1b00 424154315f4d4943524f5f43484153455f544849535f4241544348
1a050000 e9 82 5807 1700 424154315f4d4958315f4143544956455f484541444552
1b050000 c4 00 0400 1f00 424154315f4d4958315f494e544552525550545f42415443485f434f554e54
1c050000 14 8c 8400 0d00 424154315f4d4958315f4f5053
1d050000 58 83 2800 1000 424154315f4d4958315f504152414d53
1e050000 c4 00 0400 2600 424154315f4d4958315f50524f44554354494f4e5f5343414c45535f44495343485f54494d45
*/
	tag.InstanceID = binary.LittleEndian.Uint32(packet[0:]) //I think actually InstanceID, 32bit
	tag.DataType = packet[4]
	if tag.DataType == 0xC1 {
		tag.BitPosition = packet[5] & 0x07
	}
	tag.ArrayDims = (packet[5] & 0x60) >> 5 //shift right 5 bits
	tag.IsStruct = (packet[5] & 0x80) > 0
	tag.IsSystem = (packet[5] & 0x10) > 0
	//DataType is 16bit: if low byte = 0xc1, then bits 8-10 = bit position
	//bits 13-14 are array dims (0 - 3)
	// bit 15 indicates struct: in this case bits 0-11 are instanceID of template obj for
	//  structure definition
	// bit 12 indicates system tag
	length := binary.LittleEndian.Uint16(packet[8:])
	if len(programName) > 0 {
		tag.TagName = programName + "." + string(packet[10:length+10])
	} else {
		tag.TagName = string(packet[10:length+10])
	}

	return tag
}

func (plc *PLC)_multiParser(data []byte) []interface{} {
	/*
	Takes multi read reply data and returns an array of the values
	*/
	// remove the beginning of the packet because we just don't care about it
	var reply []interface{}
	stripped := data[50:]
	tagCount := int(binary.LittleEndian.Uint16(stripped[0:]))

	// get the offset values for each of the tags in the packet
	for i:=0; i<tagCount; i++ {
		loc := 2+(i*2)	//# pointer to offset
		offset := binary.LittleEndian.Uint16(stripped[loc:])
		replyStatus := stripped[offset+2]
		replyExtended := stripped[offset+3]

		//# successful reply, add the value to our list
		if replyStatus == 0 && replyExtended == 0 {
			dataTypeValue := stripped[offset+4]
			//160 is supposed to be struct?
			if dataTypeValue == 160 || dataTypeValue == 218 {
				strlen := uint16(stripped[offset+8])
				reply = append(reply, string(stripped[offset+12:offset+12+strlen]))
			} else {
				switch plc.CIPTypes[dataTypeValue].format {
				case '?':	//boolean, values are 0x00 or 0xFF
					reply = append(reply, stripped[offset+6])
				case 'b':	//SINT
					reply = append(reply, int8(stripped[offset+6]))
				case 'h':	//INT
					reply = append(reply, int16(binary.LittleEndian.Uint16(stripped[offset+6:])))
				case 'i':	//DINT
					reply = append(reply, int32(binary.LittleEndian.Uint32(stripped[offset+6:])))
				case 'q':	//LINT
					reply = append(reply, int64(binary.LittleEndian.Uint64(stripped[offset+6:])))
				case 'B':	//USINT
					reply = append(reply, stripped[offset+6])
				case 'H':	//UINT
					reply = append(reply, binary.LittleEndian.Uint16(stripped[offset+6:]))
				case 'I':	//UDINT
					reply = append(reply, binary.LittleEndian.Uint32(stripped[offset+6:]))
				case 'Q':	//LWORD
					reply = append(reply, binary.LittleEndian.Uint64(stripped[offset+6:]))
				case 'f':	//REAL
					reply = append(reply, math.Float32frombits(binary.LittleEndian.Uint32(stripped[offset+6:])))
				case 'd':	//LREAL
					reply = append(reply, math.Float64frombits(uint64(binary.LittleEndian.Uint32(stripped[offset+6:]))))
				}
			}
		} else {
			reply = append(reply, "Error")
		}
	}
	return reply
}

func (plc *PLC)_connect() bool {
	if plc.SocketConnected {
		return true
	}
	var err error

	addr := plc.IPAddress + ":" + strconv.Itoa(int(plc.Port))
	plc.Socket, err = net.Dial("tcp", addr)
	if err != nil {
		plc.SocketConnected = false
		plc.SequenceCounter = 1
		fmt.Println(err)
		return false
	}

	buf := plc._buildRegisterSession()
	retData := plc._getBytes(buf)

	if retData != nil {
		plc.SessionHandle = binary.LittleEndian.Uint32(retData[4:])
	} else {
		plc.SocketConnected = false
		fmt.Println("Failed to register session")
		return false
	}
	
	buf = plc._buildForwardOpenPacket()
	retData = plc._getBytes(buf)
	if retData != nil {
		plc.OTNetworkConnectionID = binary.LittleEndian.Uint32(retData[44:])
		plc.SocketConnected = true
	} else {
		plc.SocketConnected = false
		fmt.Println("Forward Open Failed")
		return false
	}

	return true
}

func (plc *PLC)_closeConnection() {

	closePacket := plc._buildForwardClosePacket()
	unregPacket := plc._buildUnregisterSession()
	
	plc._getBytes(closePacket)
	plc._getBytes(unregPacket) //Maybe this doesn't need response?
	
	plc.Socket.Close()
}

func (plc *PLC)_getBytes(data []byte) []byte {
	tmp := make([]byte, 1024)
	var count int
	
	plc.Socket.SetDeadline(time.Now().Add(1*time.Second))
	_, err := plc.Socket.Write(data)
	if err != nil {
		plc.SocketConnected = false
		fmt.Println("Write: "+err.Error())
		return nil
	}

	plc.Socket.SetDeadline(time.Now().Add(2*time.Second))
	count, err = plc.Socket.Read(tmp)
	if err != nil {
		//plc.SocketConnected = false
		fmt.Println("Read: "+err.Error())
		return nil
	} else {
		return tmp[:count]
	}
}

func (plc *PLC)_buildRegisterSession() []byte {
	rs := RegSession{ 
		EIPCommand: 0x0065,
		EIPLength: 0x0004,
		EIPSessionHandle: plc.SessionHandle,
		EIPStatus: 0x0000,
		EIPContext: plc.Context,
		EIPOptions: 0x0000,
		EIPProtocolVersion: 0x01,
		EIPOptionFlag: 0x00,
	}
	buf := new(bytes.Buffer)
	
	if err := binary.Write(buf, binary.LittleEndian, rs); err != nil {
		fmt.Println(err)
		return nil
	} else {
		return buf.Bytes()
	}
}

func (plc *PLC)_buildUnregisterSession() []byte {
	us := UnregSession{ 
		EIPCommand: 0x66,
		EIPLength: 0x00,
		EIPSessionHandle: plc.SessionHandle,
		EIPStatus: 0x0000,
		EIPContext: plc.Context,
		EIPOptions: 0x0000,
	}
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, us); err != nil {
		fmt.Println(err)
		return nil
	} else {
		return buf.Bytes()
	}
}

func (plc *PLC)_buildCIPForwardOpen() []byte {
	cip_fo := CIPForwardOpen{
		CIPService: 0x54,
		CIPPathSize: 0x02,
		CIPClassType: 0x20,
		CIPClass: 0x06,
		CIPInstanceType: 0x24,
		CIPInstance: 0x01,
		CIPPriority: 0x0A,
		CIPTimeoutTicks: 0x0e,
		CIPOTConnectionID: 0x20000002,
		CIPTOConnectionID: 0x20000001,
		CIPConnectionSerialNumber: plc.SerialNumber,
		CIPVendorID: plc.VendorID,
		CIPOriginatorSerialNumber: uint32(plc.OriginatorSerialNumber),
		CIPMultiplier: 0x03,
		CIPOTRPI: 0x00201234,
		CIPOTNetworkConnectionParameters: 0x43f4,
		CIPTORPI: 0x00204001,
		CIPTONetworkConnectionParameters: 0x43f4,
		CIPTransportTrigger: 0xA3,
	}
	buf := new(bytes.Buffer)
	
	if err := binary.Write(buf, binary.LittleEndian, cip_fo); err != nil {
		fmt.Println(err)
		return nil
	}
	
	connPath := [7]byte{0x00, 0x01, plc.ProcessorSlot, 0x20, 0x02, 0x24, 0x01}
	size :=(len(connPath)-1)/2
	connPath[0] = byte(size)
	
	//Not totally sure if write to buf keeps track of where it ended
	if err := binary.Write(buf, binary.LittleEndian, connPath); err != nil {
		fmt.Println(err)
		return nil
	} else {
		return buf.Bytes()
	}
	
}

func (plc *PLC)_buildCIPForwardClose() []byte {
	cip_fc := CIPForwardClose{
		CIPService: 0x4e,
		CIPPathSize: 0x02,
		CIPClassType: 0x20,
		CIPClass: 0x06,
		CIPInstanceType: 0x24,
		CIPInstance: 0x01,
		CIPPriority: 0x0A,
		CIPTimeoutTicks: 0x0e,
		CIPConnectionSerialNumber: plc.SerialNumber,
		CIPVendorID: plc.VendorID,
		CIPOriginatorSerialNumber: uint32(plc.OriginatorSerialNumber),
	}
	buf := new(bytes.Buffer)
	
	if err := binary.Write(buf, binary.LittleEndian, cip_fc); err != nil {
		fmt.Println(err)
		return nil
	}
	
	connPath := [6]byte{0x01, plc.ProcessorSlot, 0x20, 0x02, 0x24, 0x01}
	size := uint16(len(connPath)/2)
	binary.Write(buf, binary.LittleEndian, size)
	
	if err := binary.Write(buf, binary.LittleEndian, connPath); err != nil {
		fmt.Println(err)
		return nil
	} else {
		return buf.Bytes()
	}
}

func (plc *PLC)_buildEIPSendRRDataHeader(baseData []byte) []byte {
	eipRR := EIPSendRRDataHeader {
		EIPCommand: 0x6F,	   //#(H)EIP SendRRData  (Vol2 2-4.7)
		EIPLength: 16+uint16(len(baseData)),		 //#(H)
		EIPSessionHandle: plc.SessionHandle,	  //#(I)
		EIPStatus: 0x00,		   //#(I)
		EIPContext: plc.Context,	   //#(Q)
		EIPOptions: 0x00,		   //#(I)
				//#Begin Command Specific Data
		EIPInterfaceHandle: 0x00,	//#(I) Interface Handel	   (2-4.7.2)
		EIPTimeout: 0x00,		   //#(H) Always 0x00
		EIPItemCount: 0x02,		 //#(H) Always 0x02 for our purposes
		EIPItem1Type: 0x00,		 //#(H) Null Item Type
		EIPItem1Length: 0x00,		   //#(H) No data for Null Item
		EIPItem2Type: 0xB2,		 //#(H) Uconnected CIP message to follow
		EIPItem2Length: uint16(len(baseData)),  //#(H)
	}
	buf := new(bytes.Buffer)
	
	if err := binary.Write(buf, binary.LittleEndian, eipRR); err != nil {
		fmt.Println(err)
		return nil
	} else {
		return buf.Bytes()
	}
}

func (plc *PLC)_buildForwardOpenPacket() []byte {
	
	data := plc._buildCIPForwardOpen()
	rrDataHeader := plc._buildEIPSendRRDataHeader(data)
	return append(rrDataHeader, data...)
}

func (plc *PLC)_buildForwardClosePacket() []byte { 
	data := plc._buildCIPForwardClose()
	rrDataHeader := plc._buildEIPSendRRDataHeader(data)
	return append(rrDataHeader, data...)
}

func (plc *PLC)_buildTagIOI(tagName string, isBoolArray bool) []byte {
	buf := new(bytes.Buffer)
	tagArray := strings.Split(tagName, ".")

	//# this loop figures out the packet length and builds our packet
	for i:=0; i<len(tagArray); i++ {
		if strings.HasSuffix(tagArray[i],"]") {
			_, basetag, index := _tagNameParser(tagArray[i], 0)
			
			BaseTagLenBytes := len(basetag)						 //# get number of bytes
			if isBoolArray && i == len(tagArray)-1 {
				index = index/32
			}

			//# Assemble the packet
			buf.WriteByte(0x91)
			buf.WriteByte(byte(BaseTagLenBytes))
			buf.WriteString(basetag) //This might not work right
			
			if BaseTagLenBytes%2 > 0 {								  //# check for odd bytes
				BaseTagLenBytes += 1							   //# add another byte to make it even
				buf.WriteByte(0x00)
			}
			//BaseTagLenWords := BaseTagLenBytes/2					//# figure out the words for this segment

			if i < len(tagArray) {
 //			   if _,ok := index.(int); ok{
				//not isinstance(index, list) {	//checking if index is an int - requires it to change to an interface{}
					if index < 256{		  					//# if index is 1 byte...
						buf.WriteByte(0x28)
						buf.WriteByte(byte(index))
					}
					if 65536 > index && index > 255{						 //# if index is more than 1 byte...
						binary.Write(buf, binary.LittleEndian, 0x0029)
						binary.Write(buf, binary.LittleEndian, uint16(index))   //# add 2 words to packet
					}
					if index > 65535 {								//# if index is more than 4 bytes
						binary.Write(buf, binary.LittleEndian, 0x002A)
						binary.Write(buf, binary.LittleEndian, uint32(index))
					}
/*				} else {	//index is an array
					for i2=0; i2<len(index); i2++ {
						if index[i2] < 256 {								  //# if index is 1 byte...
							buf.WriteByte(0x28)
							buf.WriteByte(byte(index[i2]))   //# add one word to packet
						}
						if 65536 > index[i2] > 255 {						 //# if index is more than 1 byte...
							binary.Write(buf, binary.LittleEndian, 0x0029)
							binary.Write(buf, binary.LittleEndian, uint16(index[i2]))   //# add 2 words to packet
						}
						if index[i2] > 65535 {								//# if index is more than 4 bytes
							binary.Write(buf, binary.LittleEndian, 0x002A)
							binary.Write(buf, binary.LittleEndian, uint32(index[i2]))  //# add 2 words to packet
						}
					}
				}
*/
			}
		} else {
			_, err := strconv.Atoi(tagArray[i])
			if err != nil { //then it is not a bool index
				BaseTagLenBytes := len(tagArray[i])	//# store len of tag
				buf.WriteByte(0x91)
				buf.WriteByte(byte(BaseTagLenBytes))
				buf.WriteString(tagArray[i])
				
				if BaseTagLenBytes%2 > 0 {	//# check for odd bytes
					BaseTagLenBytes += 1	//# add another byte to make it even
					buf.WriteByte(0x00)
				}
			}
		}
	}
	
	return buf.Bytes()
}

func (plc *PLC)_addReadIOI(tagIOI []byte, elements uint16) []byte {
	buf := new(bytes.Buffer)
	
	buf.WriteByte(0x4C)
	buf.WriteByte(byte(len(tagIOI)/2))
	buf.Write(tagIOI)
	binary.Write(buf, binary.LittleEndian, elements)
	
	return buf.Bytes()
}

func (plc *PLC)_addPartialReadIOI(tagIOI []byte, elements uint16) []byte {
	buf := new(bytes.Buffer)
	
	buf.WriteByte(0x52)
	buf.WriteByte(byte(len(tagIOI)/2))
	buf.Write(tagIOI)
	
	binary.Write(buf, binary.LittleEndian, elements)
	binary.Write(buf, binary.LittleEndian, uint16(plc.Offset))
	binary.Write(buf, binary.LittleEndian, uint16(0x0000))
	
	return buf.Bytes()
}

func (plc *PLC)_buildEIPHeader(payloadLen int) []byte {
	if plc.ContextPointer == 155 {
		plc.ContextPointer = 0
	}
	EIPPayloadLength := 22 + payloadLen	//#22 bytes of command specific data + the size of the CIP Payload
	EIPConnectedDataLength := payloadLen+2	  //#Size of CIP packet plus the sequence counter

	eip := EIPHeader {
		EIPCommand: 0x70, //#(H) Send_unit_Data (vol 2 section 2-4.8)
		EIPLength: uint16(EIPPayloadLength), // #(H) Length of encapsulated command
		EIPSessionHandle: plc.SessionHandle, //#(I)Setup when session crated
		EIPStatus: 0x00,  //#(I)Always 0x00
		EIPContext: context_dict[plc.ContextPointer],
			//#Here down is command specific data
			//#For our purposes it is always 22 bytes
		EIPOptions: 0x0000,  //#(I) Always 0x00
		EIPInterfaceHandle: 0x00,  //#(I) Always 0x00
		EIPTimeout: 0x00,   //#(H) Always 0x00
		EIPItemCount: 0x02, //#(H) For our purposes always 2
		EIPItem1ID: 0xA1,   //#(H) Address (Vol2 Table 2-6.3)(2-6.2.2)
		EIPItem1Length: 0x04,  //#(H) Length of address is 4 bytes
		EIPItem1: plc.OTNetworkConnectionID,  //#(I) O->T Id
		EIPItem2ID: 0xB1,  //#(H) Connecteted Transport  (Vol 2 2-6.3.2)
		EIPItem2Length: uint16(EIPConnectedDataLength),	 //#(H) Length of CIP Payload
		EIPSequence: plc.SequenceCounter, //#(H)
	}
	
	buf := new(bytes.Buffer)
	
	plc.SequenceCounter += 1
	//plc.SequenceCounter = plc.SequenceCounter%0x10000
	
	plc.ContextPointer += 1
	
	if err := binary.Write(buf, binary.LittleEndian, eip); err != nil {
		fmt.Println(err)
		return nil
	}
	
	return buf.Bytes()
}

func (plc *PLC)_buildMultiServiceHeader() []byte {
	ms := MultiServiceHeader {
		MultiService: 0X0A,
		MultiPathSize: 0x02,
		MutliClassType: 0x20,
		MultiClassSegment: 0x02,
		MultiInstanceType: 0x24,
		MultiInstanceSegment: 0x01,
	}
	
	buf := new(bytes.Buffer)
	
	if err := binary.Write(buf, binary.LittleEndian, ms); err != nil {
		fmt.Println(err)
		return nil
	}
	return buf.Bytes()
}

func (plc *PLC)_parseReply(tag string, elements uint16, data []byte) []interface {}{
	var vals []interface{}
	var words []uint16

	_, basetag, index := _tagNameParser(tag, 0)
	datatype := plc.KnownTags[basetag].dataType
	bitCount := plc.CIPTypes[datatype].dataLen * 8

	//# if bit of word was requested
	if BitofWord(tag) {
		split_tag := strings.Split(tag, ".")
		bitPos, _ := strconv.Atoi(split_tag[len(split_tag)-1])

		wordCount := _getWordCount(uint32(bitPos), elements, bitCount)
		tmp := plc._getReplyValues(tag, wordCount, data)
		for _, val := range(tmp) {
			words = append(words, val.(uint16))
		}
		bits := plc._wordsToBits(tag, words, elements)
		for _, x := range bits {
			vals = append(vals, x)
		}
	} else if datatype == 211 {
		wordCount := _getWordCount(uint32(index), elements, bitCount)
		tmp := plc._getReplyValues(tag, wordCount, data)
		for _, val := range(tmp) {
			words = append(words, val.(uint16))
		}
		bits := plc._wordsToBits(tag, words, elements)
		for _, x := range bits {
			vals = append(vals, x)
		}
	} else {
		vals = plc._getReplyValues(tag, elements, data)
	}
	
	return vals
}

func (plc *PLC)_getReplyValues(tag string, elements uint16, data []byte) []interface{} {
	var vals []interface{}
	
	status := uint16(data[48])
	//extendedStatus := data[49]

	if status == 0 || status == 6 {
		//# parse the tag: This really isn't necessary, the reply explicitly states the datatype
		//_, basetag, index := _tagNameParser(tag, 0)
		//datatype := plc.KnownTags[basetag].dataType
		
		datatype := data[50] //50:51 technically, as uint16
		CIPFormat := plc.CIPTypes[datatype].format

		dataSize := plc.CIPTypes[datatype].dataLen
		numbytes := len(data)-dataSize
		counter := 0
		
		plc.Offset = 0
		for i := uint16(0); i<elements; i++ {
			index := 52+(counter*dataSize)
			if datatype == 160 {
			//This is a struct, wouldn't be reading a whole struct value
				index = 54+(counter*dataSize)
				NameLength := binary.LittleEndian.Uint64(data[index:])
				vals = append(vals, string(data[index+4:index+4+int(NameLength)]))
			} else if datatype == 218 {
				NameLength := data[index]
				vals = append(vals, string(data[index+1:index+1+int(NameLength)]))
			} else {
				switch CIPFormat {
				case '?':	//boolean, values come back as 0x00 or 0xFF
					vals = append(vals, data[index] > 0)
				case 'b':	//SINT
					vals = append(vals, int8(data[index]))
				case 'h':	//INT
					vals = append(vals, int16(binary.LittleEndian.Uint16(data[index:])))
				case 'i':	//DINT
					vals = append(vals, int32(binary.LittleEndian.Uint32(data[index:])))
				case 'q':	//LINT
					vals = append(vals, int64(binary.LittleEndian.Uint64(data[index:])))
				case 'B':	//USINT
					vals = append(vals, data[index])
				case 'H':	//UINT
					vals = append(vals, binary.LittleEndian.Uint16(data[index:]))
				case 'I':	//UDINT
					vals = append(vals, binary.LittleEndian.Uint32(data[index:]))
				case 'Q':	//LWORD
					vals = append(vals, binary.LittleEndian.Uint64(data[index:]))
				case 'f':	//REAL
					vals = append(vals, math.Float32frombits(binary.LittleEndian.Uint32(data[index:])))
				case 'd':	//LREAL
					vals = append(vals, math.Float64frombits(uint64(binary.LittleEndian.Uint32(data[index:]))))
				}
			}
			plc.Offset += uint16(dataSize)
			counter += 1
			
			//# re-read because the data is in more than one packet
			if index == numbytes && status == 6 {
				index = 0
				counter = 0
			}
/*	Ignoring for now: don't want to handle send/receive inside this method
				tagIOI := _buildTagIOI(plc, tag, false)
				readIOI := _addPartialReadIOI((&plc), tagIOI, elements)
				eipHeader := _buildEIPHeader((&plc), readIOI)

				self.Socket.send(eipHeader)
				data = self.Socket.recv(1024)
				status = unpack_from('<h', data, 48)[0]
				numbytes = len(data)-dataSize
*/
		}
	} else { //# didn't nail it
		var err string
		if code, ok := cipErrorCodes[status]; ok {
			err = code
		} else {
			err = "Unknown error"
		}
		vals = append(vals, "Failed to read tag: " + tag + " - " + err ) 
	}
	return vals
}

func (plc *PLC)_wordsToBits(tag string, value []uint16, count uint16) []bool {
	_, basetag, index := _tagNameParser(tag, 0)
	datatype := plc.KnownTags[basetag].dataType
	bitCount := plc.CIPTypes[datatype].dataLen * 8
	var bitPos int

	if datatype == 211 {
		bitPos = index
	} else {
		split_tag := strings.Split(tag, ".")
		bitPos, _ = strconv.Atoi(split_tag[len(split_tag)-1])
	}
	
	var ret []bool
	for _, v := range value {
		for i:=0; i<bitCount; i++ {
			ret = append(ret, BitValue(v, uint16(i)))
		}
	}
	return ret[bitPos:bitPos+int(count)]
}

func (plc *PLC)_initialRead(tag string, baseTag string) bool {
	//# if a tag alread exists, return True
	if _, ok := plc.KnownTags[baseTag]; ok {
		return true
	}
	
	tmp := make([]byte, 1024)

	tagData := plc._buildTagIOI(baseTag, false)
	readIOI := plc._addPartialReadIOI(tagData, 1)
	eipHeader := plc._buildEIPHeader(len(readIOI))
	readRequest := append(eipHeader, readIOI...)
	
	plc.Socket.SetDeadline(time.Now().Add(1*time.Second))
	//# send our tag read request
	plc.Socket.Write(readRequest)
	
	plc.Socket.SetDeadline(time.Now().Add(5*time.Second))
	_, err := plc.Socket.Read(tmp)
	
	if err != nil {
		plc.SocketConnected = false
		return false
	}
	status := tmp[48]

	//# make sure it was successful
	if status == 0 || status == 6 {
		dataType := tmp[50]
		dataLen := binary.LittleEndian.Uint16(tmp[2:])  //# this is really just used for STRING
		plc.KnownTags[baseTag] = TagMap{dataType: dataType, dataLen: int(dataLen)}
		return true
	} else {
		fmt.Println("Failed to read initial tag: " + strconv.Itoa(int(status))) 
		return false
	}
}

func _tagNameParser(tag string, offset uint16) (string, string, int) {
	bt := tag
	ind := 0
	//var index interface{}
	
	if strings.HasSuffix(tag, "]") {
		pos := strings.LastIndex(tag, "[") //# find position of [
		bt = tag[:pos-1]			//# remove [x]: result=SuperDuper
		ind_s := tag[pos+1:len(tag)-2]		   // # strip the []: result=x
		s := strings.Split(ind_s, ",")			//# split so we can check for multi dimensin array
		if len(s) == 1 {
			ind, _ = strconv.Atoi(ind_s)
			//index = ind
		} else {
			//# if we have a multi dim array, return the index
			var ind_array []int
			for i:=0; i<len(s); i++ {
				ind, _ = strconv.Atoi(s[i])
				ind_array = append(ind_array, ind)
			}
			//index = ind_array
		}
	}
	
	return tag, bt, ind
}

func _getBitOfWord(tag string, value uint16) bool {
	split_tag := strings.Split(tag, ".")
	bitPos, _ := strconv.Atoi(split_tag[len(split_tag)-1])
	returnvalue := false
	
	if bitPos<=31 {
		returnvalue = BitValue(value, uint16(bitPos))
	}
	
	return returnvalue
}

func _getWordCount(start uint32, length uint16, bits int) uint16 {
	totalBits := start+uint32(length)
	wordCount := totalBits / uint32(bits)
	if totalBits % 32 > 0 {
		wordCount += 1
	}
	return uint16(wordCount)
}

func BitValue(value uint16, bitno uint16) bool {
	mask := uint16(1) << bitno
	if (value & mask > 0) {
		return true
	} else {
		return false
	}
}

func BitofWord(tag string) bool {
	s := strings.Split(tag, ".")
	if _, err := strconv.Atoi(s[len(s)-1]); err == nil {
		return true
	} else {
		return false
	}
}

func (plc *PLC)Read(args ...interface{}) []interface{} {
	/*
	We have two options for reading depending on
	the arguments, read a single tag, or read an array
	*/
	tag := ""
	elements := 1
	
	for i,arg := range args {
		switch i {
		case 0:	//tag
			if t, ok := arg.(string); !ok {
				panic("1st arg should be tag name, type string.")
			} else {
				tag = t
			}
		case 1:	//elements
			if e, ok := arg.(int); !ok {
				panic("2nd arg should be elements, type int.")
			} else {
				elements = e
			}
		default:
			panic("Too many arguments supplied.")
		}
	}
	
	if len(tag)>0 {
		return plc._readTag(tag, uint16(elements))
	} else {
		return nil
	}
}

func (plc *PLC)MultiRead(args []string) []interface{} {
        /*
        Read multiple tags in one request
        */
        return plc._multiRead(args)
}

func (plc *PLC)GetPLCTime() time.Time {
        /*
        Get the PLC's clock time
        */
        return plc._getPLCTime()
}

func (plc *PLC)GetTagList() []LGXTag {
        /*
        Retrieves the tag list from the PLC
        */
        return plc._getTagList()
}

func (plc *PLC)FilterTagList(dataType byte) []string {
	/*
	Using 0 as "no filter"
	*/
	var result []string
	for _, tag := range plc.TagList {
		if dataType == 0 || tag.DataType == dataType {
			result = append(result, tag.TagName)
		}
	}
	return result
}

func (plc *PLC)PrintTagList(dataType byte) {
	/*
	Using 0 as "no filter"
	*/
	fmt.Printf("Offset\tType \tStruct\tSystem\tDims\tTag Name\n")
	for _, tag := range plc.TagList {
		if dataType == 0 || tag.DataType == dataType {
			fmt.Printf("%v\t%v\t%v\t%v\t%v\t%v\n", tag.InstanceID, tag.DataType, tag.IsStruct, tag.IsSystem, tag.ArrayDims, tag.TagName)
		}
	}
}

func (plc *PLC)Close() {
        /*
        Close the connection to the PLC
        */
        plc._closeConnection()
}

