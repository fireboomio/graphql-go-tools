package plan

import (
	"fmt"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/resolve"
	"golang.org/x/exp/slices"
	"strings"
)

func (v *Visitor) resolveTransformForObjectField(ref int) {
	var currentField *resolve.Field
	if strVal, ok := v.currentField.Value.(*resolve.String); ok && slices.Contains(strVal.Path, resolve.QueryRawKey) {
		currentField = v.currentField
	}
	index, ok := v.currentFieldIndexes[ref]
	if ok && index < len(v.currentFields) {
		currentField = v.currentFields[index].popField
	}
	if currentField == nil {
		return
	}

	v.resolveTransformForField(ref, currentField)
}

func (v *Visitor) resolveTransformForField(ref int, field *resolve.Field) {
	for _, i := range v.Operation.Fields[ref].Directives.Refs {
		if v.Operation.DirectiveNameString(i) != "transform" {
			continue
		}

		field.TransformRequired = true
		field.TransformDirective.Defined = true
		field.TransformDirective.ArrayWalked = field.Value.NodeKind() == resolve.NodeKindArray
		if value, ok := v.Operation.DirectiveArgumentValueByName(i, resolve.TransformArgGet); ok {
			for _, item := range strings.Split(v.Operation.ValueContentString(value), ".") {
				if item != "[]" && item != "" {
					field.TransformDirective.Get = append(field.TransformDirective.Get, item)
				}
			}
		}
		v.resolveTransformForFieldChildren(field.Value, &field.TransformDirective, 0)
		if value, ok := v.Operation.DirectiveArgumentValueByName(i, resolve.TransformArgMath); ok {
			if !field.TransformDirective.ArrayWalked {
				v.Walker.StopWithInternalErr(fmt.Errorf("@transform with math can only be used on arrays"))
				return
			}
			transformMath := resolve.TransformMath(v.Operation.ValueContentString(value))
			transformGetNodeKind := field.TransformDirective.GetNode.NodeKind()
			switch transformMath {
			case resolve.TransformMathMax, resolve.TransformMathMin, resolve.TransformMathSum, resolve.TransformMathAvg:
				if transformGetNodeKind != resolve.NodeKindInteger && transformGetNodeKind != resolve.NodeKindFloat {
					v.Walker.StopWithInternalErr(fmt.Errorf("@transform math [%s] can only be used on integer or float values", transformMath))
					return
				}
			case resolve.TransformMathFirst, resolve.TransformMathLast, resolve.TransformMathCount:
			default:
				v.Walker.StopWithInternalErr(fmt.Errorf("not support transform math [%s]", transformMath))
				return
			}
			field.TransformDirective.Math = transformMath
		}
		return
	}
}

func (v *Visitor) resolveTransformForFieldChildren(fieldValue resolve.Node, transform *resolve.TransformDirective, pathIndex int) bool {
	if pathIndex == len(transform.Get) {
		transform.GetNode = fieldValue
		transform.ArrayWalked = transform.ArrayWalked || fieldValue.NodeKind() == resolve.NodeKindArray
		return false
	}

	switch ret := fieldValue.(type) {
	case *resolve.Array:
		transform.ArrayWalked = true
		ret.TransformItemRequired = v.resolveTransformForFieldChildren(ret.Item, transform, pathIndex) || ret.TransformItemRequired
		return true
	case *resolve.Object:
		if ret.TransformFieldRequired {
			v.Walker.StopWithInternalErr(fmt.Errorf("repeat path [%s] @transform", strings.Join(transform.Get[:pathIndex+1], ".")))
			return false
		}
		fieldName := transform.Get[pathIndex]
		for i := range ret.Fields {
			if string(ret.Fields[i].Name) == fieldName {
				ret.TransformFieldIndex = i
				ret.TransformFieldRequired = true
				ret.Fields[i].TransformRequired = v.resolveTransformForFieldChildren(ret.Fields[i].Value, transform, pathIndex+1) || ret.Fields[i].TransformRequired
				return true
			}
		}
		v.Walker.StopWithInternalErr(fmt.Errorf("invalid path [%s] @transform", strings.Join(transform.Get[:pathIndex+1], ".")))
		return false
	case *resolve.String:
		if slices.Contains(ret.Path, resolve.QueryRawKey) || ret.UnescapeResponseJson {
			ret.TransformFieldName = transform.Get[pathIndex]
			transform.ArrayWalked = transform.ArrayWalked || !ret.FirstRawResult
			v.resolveTransformForFieldChildren(&resolve.Null{}, transform, pathIndex+1)
			return true
		}
	}
	v.Walker.StopWithInternalErr(fmt.Errorf("invalid nodeKind [%d] on path [%s] @transform", fieldValue.NodeKind(), strings.Join(transform.Get[:pathIndex], ".")))
	return false
}
