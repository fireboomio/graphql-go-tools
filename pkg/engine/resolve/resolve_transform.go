package resolve

import (
	"bytes"
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/spf13/cast"
	"github.com/wundergraph/graphql-go-tools/pkg/lexer/literal"
	"slices"
	"strconv"
)

var (
	TransformArgGet  = []byte("get")
	TransformArgMath = []byte("math")
	TransformEnabled = "transformEnabled"
)

type TransformDirective struct {
	Defined     bool
	Get         []string
	Math        TransformMath
	GetNode     Node
	ArrayWalked bool
}

type TransformMath string

const (
	TransformMathMax   TransformMath = "MAX"
	TransformMathMin   TransformMath = "MIN"
	TransformMathSum   TransformMath = "SUM"
	TransformMathAvg   TransformMath = "AVG"
	TransformMathCount TransformMath = "COUNT"
	TransformMathFirst TransformMath = "FIRST"
	TransformMathLast  TransformMath = "LAST"
)

func (r *Resolver) resolveTransformNodeData(node Node, nodeData []byte) ([]byte, bool) {
	if bytes.Equal(nodeData, literal.NULL) {
		return literal.NULL, false
	}

	switch ret := node.(type) {
	case *Array:
		if !ret.TransformItemRequired || bytes.Equal(nodeData, literal.ZeroArrayValue) {
			return nodeData, false
		}

		return r.resolveTransformArrayData(ret.Item, nodeData), true
	case *Object:
		if !ret.TransformFieldRequired {
			return nodeData, false
		}
		data, dataType, dataOffset, _ := jsonparser.Get(nodeData, string(ret.Fields[ret.TransformFieldIndex].Name))
		switch dataType {
		case jsonparser.String:
			data = nodeData[dataOffset-len(data)-2 : dataOffset]
		case jsonparser.NotExist:
			data = ret.NodeZeroValue()
		}
		return data, true
	case *String:
		if len(ret.TransformFieldName) == 0 {
			return nodeData, false
		}
		if ret.FirstRawResult || bytes.HasPrefix(nodeData, literal.LBRACE) && bytes.HasSuffix(nodeData, literal.RBRACE) {
			data, dataType, dataOffset, _ := jsonparser.Get(nodeData, ret.TransformFieldName)
			switch dataType {
			case jsonparser.String:
				data = nodeData[dataOffset-len(data)-2 : dataOffset]
			case jsonparser.NotExist:
				data = ret.NodeZeroValue()
			}
			return data, true
		}
		if bytes.Equal(nodeData, literal.ZeroArrayValue) {
			return nodeData, false
		}
		retCopy := *ret
		retCopy.FirstRawResult = true
		return r.resolveTransformArrayData(&retCopy, nodeData), true
	default:
		return nodeData, false
	}
}

func (r *Resolver) resolveTransformArrayData(arrayitem Node, arrayData []byte) []byte {
	dataBuf := r.getBufPair()
	defer r.freeBufPair(dataBuf)
	first := true
	dataBuf.Data.WriteBytes(literal.LBRACK)
	_, _ = jsonparser.ArrayEach(arrayData, func(itemData []byte, itemType jsonparser.ValueType, itemOffset int, _ error) {
		if itemType == jsonparser.String {
			itemData = arrayData[itemOffset-len(itemData)-2 : itemOffset]
		}
		if first {
			first = false
		} else {
			dataBuf.Data.WriteBytes(literal.COMMA)
		}
		itemData, _ = r.resolveTransformNodeData(arrayitem, itemData)
		dataBuf.Data.WriteBytes(itemData)
	})
	dataBuf.Data.WriteBytes(literal.RBRACK)
	return dataBuf.Data.Bytes()
}

func (r *Resolver) resolveTransformFieldBuf(ctx *Context, field *Field, fieldBuf *BufPair) error {
	if enabled, ok := ctx.Value(TransformEnabled).(bool); !ok || !enabled || !field.TransformRequired {
		return nil
	}

	originData := fieldBuf.Data.Bytes()
	transformedData, transformed := r.resolveTransformNodeData(field.Value, originData)
	defer func() {
		if transformed {
			fieldBuf.Data.Reset()
			fieldBuf.Data.WriteBytes(transformedData)
		}
	}()
	if len(field.TransformDirective.Math) == 0 {
		return nil
	}

	transformed = true
	if len(transformedData) <= len(literal.ZeroArrayValue) || bytes.Equal(transformedData, literal.NULL) {
		if nodeZero, ok := field.TransformDirective.GetNode.(NodeZeroValue); ok {
			transformedData = nodeZero.NodeZeroValue()
		} else {
			transformedData = literal.NULL
		}
		return nil
	}

	var items [][]byte
	_, _ = jsonparser.ArrayEach(transformedData, func(itemData []byte, dataType jsonparser.ValueType, offset int, err error) {
		if dataType == jsonparser.String {
			itemData = transformedData[offset-len(itemData)-2 : offset]
		}
		items = append(items, itemData)
	})
	switch field.TransformDirective.Math {
	case TransformMathMax:
		transformedData = slices.MaxFunc(items, bytes.Compare)
	case TransformMathMin:
		transformedData = slices.MinFunc(items, bytes.Compare)
	case TransformMathSum, TransformMathAvg:
		var numberValue float64
		for j := range items {
			numberValue += cast.ToFloat64(string(items[j]))
		}
		if numberValue != 0 {
			if field.TransformDirective.Math == TransformMathAvg {
				numberValue = numberValue / float64(len(items))
			}
			transformedData = []byte(fmt.Sprintf(`%f`, numberValue))
		} else {
			transformedData = literal.ZeroNumberValue
		}
	case TransformMathCount:
		transformedData = []byte(strconv.Itoa(len(items)))
	case TransformMathFirst:
		transformedData = items[0]
	case TransformMathLast:
		transformedData = items[len(items)-1]
	default:
		return fmt.Errorf("not support transform math [%s]", field.TransformDirective.Math)
	}
	return nil
}
