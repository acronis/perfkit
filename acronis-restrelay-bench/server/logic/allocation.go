package logic

import (
	"errors"
	"fmt"
	"unsafe"
)

type allocationAction struct {
	size int64
}

type allocationStruct struct {
	RandomFieldInt    int
	RandomFieldString string
}

func (allocationArgs *allocationAction) perform() error {
	res := make([]allocationStruct, 0)

	var structSize = unsafe.Sizeof(allocationStruct{})
	var iterations = allocationArgs.size / int64(structSize)

	for i := int64(0); i < iterations; i++ {
		res = append(res, allocationStruct{ //nolint:staticcheck
			RandomFieldInt:    2,
			RandomFieldString: "test string",
		})
	}

	return nil
}

func (allocationArgs *allocationAction) parseParameters(params map[string]string) error {
	var size, ok = params["size"]
	if !ok {
		return errors.New("size parameter is missing")
	}

	var err error
	allocationArgs.size, err = parseFileSize(size)
	if err != nil {
		return fmt.Errorf("failed conversion string to int in AllocationArguments with: %v", err)
	}

	return nil
}
