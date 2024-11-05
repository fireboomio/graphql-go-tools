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
	Defined bool
	Get     []string
	Math    TransformMath
	GetNode Node
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

func (r *Resolver) resolveTransform(ctx *Context, field *Field, fieldBuf *BufPair) error {
	if !field.TransformDirective.Defined || field.TransformDirective.GetNode == nil {
		return nil
	}
	if enabled, ok := ctx.Value(TransformEnabled).(bool); !ok || !enabled {
		return nil
	}

	fieldData := fieldBuf.Data.Bytes()
	var fieldDataModified bool
	defer func() {
		if fieldDataModified {
			fieldBuf.Data.Reset()
			fieldBuf.Data.WriteBytes(fieldData)
		}
	}()
	if len(field.TransformDirective.Get) > 0 {
		if field.Value.NodeKind() == NodeKindArray {
			if len(fieldData) > len(literal.ZeroArrayValue) {
				fieldItemBuf := r.getBufPair()
				defer r.freeBufPair(fieldItemBuf)
				fieldItemBuf.Data.WriteBytes(literal.LBRACK)
				firstItem := true
				jsonparser.ArrayEach(fieldData, func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
					if dataType == jsonparser.String {
						value = fieldData[offset-len(value)-2 : offset]
					}
					if firstItem {
						firstItem = false
					} else {
						fieldItemBuf.Data.WriteBytes(literal.COMMA)
					}
					fieldItemBuf.Data.WriteBytes(value)
				}, field.TransformDirective.Get...)
				fieldItemBuf.Data.WriteBytes(literal.RBRACK)
				fieldData, fieldDataModified = fieldItemBuf.Data.Bytes(), true
			}
		} else {
			value, valueType, offset, _ := jsonparser.Get(fieldData, field.TransformDirective.Get...)
			if valueType == jsonparser.String {
				value = fieldData[offset-len(value)-2 : offset]
			}
			fieldData, fieldDataModified = value, true
		}
	}
	if len(field.TransformDirective.Math) == 0 {
		return nil
	}

	fieldDataModified = true
	if len(fieldData) <= len(literal.ZeroArrayValue) {
		if nodeZero, ok := field.TransformDirective.GetNode.(NodeZeroValue); ok {
			fieldData = nodeZero.NodeZeroValue()
		} else {
			fieldData = literal.NULL
		}
		return nil
	}

	var items [][]byte
	jsonparser.ArrayEach(fieldData, func(itemData []byte, dataType jsonparser.ValueType, offset int, err error) {
		if dataType == jsonparser.String {
			itemData = fieldData[offset-len(itemData)-2 : offset]
		}
		items = append(items, itemData)
	})
	switch field.TransformDirective.Math {
	case TransformMathMax:
		fieldData = slices.MaxFunc(items, bytes.Compare)
	case TransformMathMin:
		fieldData = slices.MinFunc(items, bytes.Compare)
	case TransformMathSum, TransformMathAvg:
		var numberValue float64
		for j := range items {
			numberValue += cast.ToFloat64(string(items[j]))
		}
		if numberValue != 0 {
			if field.TransformDirective.Math == TransformMathAvg {
				numberValue = numberValue / float64(len(items))
			}
			fieldData = []byte(fmt.Sprintf(`%f`, numberValue))
		} else {
			fieldData = literal.ZeroNumberValue
		}
	case TransformMathCount:
		fieldData = []byte(strconv.Itoa(len(items)))
	case TransformMathFirst:
		fieldData = items[0]
	case TransformMathLast:
		fieldData = items[len(items)-1]
	default:
		fieldDataModified = false
	}
	return nil
}
