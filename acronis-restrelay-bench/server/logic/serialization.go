package logic

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"sync"

	jsoniter "github.com/json-iterator/go"
)

var quickJSON = jsoniter.ConfigCompatibleWithStandardLibrary

// UseJsoniter is a flag
var UseJsoniter = false

// SerializationCache is a non-cleared concurrent safe cache of Serialize objects and marshalled strings
type SerializationCache struct {
	serializableMutex sync.Mutex
	serializedMutex   sync.Mutex
	serializable      map[serializationAction]Serializable
	serialized        map[serializationAction][]byte
}

var (
	cache = SerializationCache{
		serializableMutex: sync.Mutex{},
		serializedMutex:   sync.Mutex{},
		serializable:      make(map[serializationAction]Serializable),
		serialized:        make(map[serializationAction][]byte),
	}
)

// GetSerializable locks mutex on map with serializables and creates new if there is no appropriate value in cache
func (cache *SerializationCache) GetSerializable(args serializationAction) Serializable {
	cache.serializableMutex.Lock()
	defer cache.serializableMutex.Unlock()

	serializable, ok := cache.serializable[args]
	if !ok {
		serializable = buildSerializable(args)
		cache.serializable[args] = serializable
	}

	return serializable
}

// GetSerialized locks mutex on map with serialized strings and creates new if there is no appropriate value in cache
func (cache *SerializationCache) GetSerialized(args serializationAction) ([]byte, error) {
	cache.serializedMutex.Lock()
	defer cache.serializedMutex.Unlock()

	serialized, ok := cache.serialized[args]
	if !ok {
		serializable := cache.GetSerializable(args)
		var str []byte
		var err error
		if UseJsoniter {
			str, err = quickJSON.Marshal(serializable)
		} else {
			str, err = json.Marshal(serializable)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to marshal serializable: %v", err)
		}
		serialized = str
		cache.serialized[args] = serialized
	}

	return serialized, nil
}

// Serializable is a json-prototype of marshalling object
type Serializable struct {
	StringField      string
	BooleanField     bool
	IntegerField     int
	FloatField       float32
	ObjectArrayField []Serializable
}

type serializationAction struct {
	TreeDepth int
	TreeWidth int
}

func (s *serializationAction) Validate() error {
	if s.TreeDepth < 1 || s.TreeWidth < 1 {
		return errors.New("TreeDepth and Tree width should be > 0")
	}

	return nil
}

type parseAction serializationAction

func (p *parseAction) Validate() error {
	if p.TreeDepth < 1 || p.TreeWidth < 1 {
		return errors.New("TreeDepth and Tree width should be > 0")
	}

	return nil
}

func buildSerializable(args serializationAction) Serializable {
	root := Serializable{
		StringField:  "Serializable",
		BooleanField: true,
		IntegerField: 12345,
		FloatField:   11.1234,
	}
	build(args, 0, &root)

	return root
}

func build(args serializationAction, currentDepth int, parent *Serializable) {
	parent.ObjectArrayField = make([]Serializable, args.TreeWidth)
	for i := 0; i < args.TreeWidth; i++ {
		parent.ObjectArrayField[i] = Serializable{
			StringField:  "Serializable",
			BooleanField: true,
			IntegerField: 12345,
			FloatField:   11.1234,
		}

		if currentDepth < args.TreeDepth-1 {
			build(args, currentDepth+1, &parent.ObjectArrayField[i])
		}
	}
}

func (s *serializationAction) perform() error {
	var err = s.Validate()
	if err != nil {
		return err
	}

	var root = cache.GetSerializable(*s)

	if UseJsoniter {
		_, err = quickJSON.Marshal(root)
	} else {
		_, err = json.Marshal(root)
	}
	if err != nil {
		return fmt.Errorf("failed to perform serialization: %v", err)
	}

	return nil
}

func (p *parseAction) perform() error {
	err := p.Validate()
	if err != nil {
		return err
	}

	str, err := cache.GetSerialized(serializationAction(*p))
	if err != nil {
		return err
	}

	var deser Serializable

	if UseJsoniter {
		err = quickJSON.Unmarshal(str, &deser)
	} else {
		err = json.Unmarshal(str, &deser)
	}

	if err != nil {
		return fmt.Errorf("failed to perform parsing: %v", err)
	}

	return nil
}

func (s *serializationAction) parseParameters(params map[string]string) error {
	var depth, width string
	var ok bool

	if depth, ok = params["depth"]; !ok {
		return errors.New("depth parameter is missing")
	}

	if width, ok = params["width"]; !ok {
		return errors.New("width parameter is missing")
	}

	var err error
	s.TreeDepth, err = strconv.Atoi(depth)
	if err != nil {
		return fmt.Errorf("failed conversion string to int in SerializationArguments with: %v", err)
	}

	s.TreeWidth, err = strconv.Atoi(width)
	if err != nil {
		return fmt.Errorf("failed conversion string to int in SerializationArguments with: %v", err)
	}

	return nil
}

func (p *parseAction) parseParameters(params map[string]string) error {
	var depth, width string
	var ok bool

	if depth, ok = params["depth"]; !ok {
		return errors.New("depth parameter is missing")
	}

	if width, ok = params["width"]; !ok {
		return errors.New("width parameter is missing")
	}

	var err error
	p.TreeDepth, err = strconv.Atoi(depth)
	if err != nil {
		return fmt.Errorf("failed conversion string to int in ParseArguments with: %v", err)
	}

	p.TreeWidth, err = strconv.Atoi(width)
	if err != nil {
		return fmt.Errorf("failed conversion string to int in ParseArguments with: %v", err)
	}

	return nil
}

// getClosestDepthAndWidth returns the closest depth and width for the given size
// This function is needed because we want to keep cache for jsons for forwarding requests with them
func getClosestDepthAndWidth(size int) (int, int) {
	type SizeInfo struct {
		Depth int
		Width int
		Size  int
	}

	var sizesCache = map[int]SizeInfo{
		100:        {1, 1, 230},       // 100B
		1024:       {1, 8, 1049},      // 1KB
		2048:       {3, 2, 1733},      // 2KB
		4096:       {2, 5, 3608},      // 4KB
		8192:       {2, 8, 8513},      // 8KB
		16384:      {6, 2, 14669},     // 16KB
		32768:      {3, 6, 30173},     // 32KB
		65536:      {3, 8, 68225},     // 64KB
		131072:     {6, 3, 126788},    // 128KB
		262144:     {4, 7, 326516},    // 256KB
		524288:     {4, 8, 545921},    // 512KB
		1048576:    {5, 6, 1087061},   // 1MB
		2097152:    {6, 5, 2273408},   // 2MB
		4194304:    {5, 8, 4367489},   // 4MB
		8388608:    {5, 9, 7750166},   // 8MB
		16777216:   {6, 7, 16000244},  // 16MB
		33554432:   {6, 8, 34940033},  // 32MB
		67108864:   {6, 9, 69751616},  // 64MB
		134217728:  {7, 7, 112001828}, // 128MB
		268435456:  {7, 8, 279520385}, // 256MB
		536870912:  {7, 9, 627764666}, // 512MB
		1073741824: {7, 9, 627764666}, // 1GB
	}

	if info, found := sizesCache[size]; found {
		return info.Depth, info.Width
	}

	sizeTable := []SizeInfo{
		{1, 1, 230}, {1, 2, 347}, {1, 3, 464}, {1, 4, 581},
		{1, 5, 698}, {1, 6, 815}, {1, 7, 932}, {1, 8, 1049}, {1, 9, 1166},
		{2, 1, 344}, {2, 2, 809}, {2, 3, 1508}, {2, 4, 2441},
		{2, 5, 3608}, {2, 6, 5009}, {2, 7, 6644}, {2, 8, 8513}, {2, 9, 10616},
		{3, 1, 458}, {3, 2, 1733}, {3, 3, 4640}, {3, 4, 9881},
		{3, 5, 18158}, {3, 6, 30173}, {3, 7, 46628}, {3, 8, 68225}, {3, 9, 95666},
		{4, 1, 572}, {4, 2, 3581}, {4, 3, 14036}, {4, 4, 39641},
		{4, 5, 90908}, {4, 6, 181157}, {4, 7, 326516}, {4, 8, 545921}, {4, 9, 861116},
		{5, 1, 686}, {5, 2, 7277}, {5, 3, 42224}, {5, 4, 158681},
		{5, 5, 454658}, {5, 6, 1087061}, {5, 7, 2285732}, {5, 8, 4367489}, {5, 9, 7750166},
		{6, 1, 800}, {6, 2, 14669}, {6, 3, 126788}, {6, 4, 634841},
		{6, 5, 2273408}, {6, 6, 6522485}, {6, 7, 16000244}, {6, 8, 34940033}, {6, 9, 69751616},
		{7, 1, 914}, {7, 2, 29453}, {7, 3, 380480}, {7, 4, 2539481},
		{7, 5, 11367158}, {7, 6, 39135029}, {7, 7, 112001828}, {7, 8, 279520385}, {7, 9, 627764666},
	}

	closestDepth, closestWidth := 0, 0
	minDiff := math.MaxInt32

	for _, info := range sizeTable {
		diff := int(math.Abs(float64(size - info.Size)))
		if diff < minDiff {
			minDiff = diff
			closestDepth = info.Depth
			closestWidth = info.Width
		}
	}

	return closestDepth, closestWidth
}
