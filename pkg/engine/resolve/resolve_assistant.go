package resolve

import (
	"fmt"
	"github.com/buger/jsonparser"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/plan"
	"github.com/wundergraph/graphql-go-tools/pkg/fastbuffer"
	"golang.org/x/exp/slices"
	"strings"
)

func (r *Resolver) formatDateTime(ctx *Context, str *String, value []byte) []byte {
	if str.DateFormatArguments == nil || ctx.DateFormatFunc == nil {
		return value
	}

	return []byte(ctx.DateFormatFunc(str.DateFormatArguments, string(value)))
}

func (r *Resolver) setResultSetSkipData(ctx *Context, object *Object, set *resultSet) {
	set.skipBufferIds = make(map[int]bool)
	set.delayFetchBufferFuncs = make(map[int]func(*Context, []byte) error)
	set.skipBufferFieldZeroValues = make(map[int]map[*Field]*NodeZeroValue)
	for i := range object.Fields {
		field := object.Fields[i]
		if !field.HasBuffer {
			continue
		}

		skipFieldZeroValues := make(map[*Field]*NodeZeroValue)
		searchSkipFieldsFunc := func(_ctx *Context, _ []byte) error {
			if field.skipRequired(_ctx) || r.searchSkipFields(_ctx, skipFieldZeroValues, field.Value) {
				set.skipBufferIds[field.BufferID] = true
			}
			if len(skipFieldZeroValues) > 0 {
				set.skipBufferFieldZeroValues[field.BufferID] = skipFieldZeroValues
			}
			return nil
		}
		if field.WaitExportedRequired || field.WaitExportedRequiredFunc != nil && field.WaitExportedRequiredFunc(ctx) {
			set.delayFetchBufferFuncs[field.BufferID] = searchSkipFieldsFunc
		} else {
			_ = searchSkipFieldsFunc(ctx, nil)
		}
	}
}

func (r *Resolver) searchSkipFields(ctx *Context, skipFieldZeroValues map[*Field]*NodeZeroValue, node Node, parent ...string) (skipAll bool) {
	switch value := node.(type) {
	case *Array:
		skipAll = r.searchSkipFields(ctx, skipFieldZeroValues, value.Item, parent...)
	case *Object:
		var objectSkipFieldCount int
		objectJsonPath := append(parent, value.NodePath()...)
		objectJsonPathLen := len(objectJsonPath)
		for _, item := range value.Fields {
			if item.HasBuffer {
				continue
			}
			nodeSkip, ok := item.Value.(NodeSkip)
			if !ok {
				continue
			}

			itemSkipPath := nodeSkip.NodePath()
			itemSkipJsonPath := make([]string, objectJsonPathLen+len(itemSkipPath))
			copy(itemSkipJsonPath, objectJsonPath)
			copy(itemSkipJsonPath[objectJsonPathLen:], itemSkipPath)
			itemSkip := item.skipRequired(ctx)
			itemSkipAll := r.searchSkipFields(ctx, skipFieldZeroValues, item.Value, itemSkipJsonPath...)
			if !itemSkip && !itemSkipAll {
				continue
			}

			objectSkipFieldCount++
			skipFieldZeroValues[item] = &NodeZeroValue{
				Path:      itemSkipPath,
				JsonPath:  itemSkipJsonPath,
				ZeroValue: nodeSkip.NodeZeroValue(!itemSkip && itemSkipAll),
			}
		}
		skipAll = objectSkipFieldCount == len(value.Fields)
	}
	return
}

func (r *Resolver) skipOrSetDelayFunc(fetch *SingleFetch, set *resultSet, fetchFunc func(*Context, []byte) error) (skip bool) {
	if _, skip = set.skipBufferIds[fetch.BufferId]; skip {
		return
	}

	delayFunc, skip := set.delayFetchBufferFuncs[fetch.BufferId]
	if skip {
		set.delayFetchBufferFuncs[fetch.BufferId] = func(ctx *Context, data []byte) error {
			_ = delayFunc(ctx, data)
			if _, skip = set.skipBufferIds[fetch.BufferId]; skip {
				return nil
			}
			return fetchFunc(ctx, data)
		}
	}
	return
}

func (r *Resolver) runDelayFuncOrSkip(ctx *Context, field *Field, data []byte, objectBuf *BufPair, set *resultSet) (fieldData []byte, skip bool, err error) {
	if delayFunc, ok := set.delayFetchBufferFuncs[field.BufferID]; ok {
		if err = delayFunc(ctx, data); err != nil {
			return
		}
		if fieldBuffer, ok := set.buffers[field.BufferID]; ok {
			r.MergeBufPairErrors(fieldBuffer, objectBuf)
		}
	}
	if _, skip = set.skipBufferIds[field.BufferID]; skip {
		return
	}

	buffer, ok := set.buffers[field.BufferID]
	if ok {
		fieldData = buffer.Data.Bytes()
		ctx.resetResponsePathElements()
		ctx.lastFetchID = field.BufferID
	}
	return
}

func (r *Resolver) setZeroValueOrSkip(ctx *Context, field *Field, data []byte) (fieldData []byte, skip bool) {
	if nodeZero, ok := ctx.skipFieldZeroValues[field]; ok {
		if skip = nodeZero.ZeroValue == nil; skip {
			return
		}

		if len(nodeZero.Path) > 0 {
			fieldData, _ = jsonparser.Set(data, nodeZero.ZeroValue, nodeZero.Path...)
		} else {
			fieldData = nodeZero.ZeroValue
		}
		return
	}

	if skip = field.skipRequired(ctx); !skip {
		fieldData = data
	}
	return
}

func (f *Field) skipRequired(ctx *Context) (skipRequired bool) {
	if skipDirective := f.SkipDirective; skipDirective.Defined {
		var skipEffective []bool
		if ifName := skipDirective.VariableName; len(ifName) > 0 {
			skip, skipErr := jsonparser.GetBoolean(ctx.Variables, ifName)
			skipEffective = append(skipEffective, skipErr == nil && skip)
		}
		if expression := skipDirective.Expression; len(expression) > 0 && ctx.RuleEvaluate != nil {
			if skipDirective.ExpressionIsVariable {
				expression, _ = jsonparser.GetString(ctx.Variables, expression)
			}
			skipEffective = append(skipEffective, ctx.RuleEvaluate(ctx.Variables, expression))
		}
		if skipRequired = !slices.Contains(skipEffective, false); skipRequired {
			return
		}
	}
	if includeDirective := f.IncludeDirective; includeDirective.Defined {
		var skipEffective []bool
		if ifName := includeDirective.VariableName; len(ifName) > 0 {
			include, includeErr := jsonparser.GetBoolean(ctx.Variables, ifName)
			skipEffective = append(skipEffective, includeErr != nil || !include)
		}
		if expression := includeDirective.Expression; len(expression) > 0 && ctx.RuleEvaluate != nil {
			if includeDirective.ExpressionIsVariable {
				expression, _ = jsonparser.GetString(ctx.Variables, expression)
			}
			skipEffective = append(skipEffective, !ctx.RuleEvaluate(ctx.Variables, expression))
		}
		if skipRequired = slices.Contains(skipEffective, true); skipRequired {
			return
		}
	}
	return
}

func (f *Field) SetWaitExportedRequiredForDirective(exportedVariables []string) {
	f.NoneExportedBefore = len(exportedVariables) == 0
	if f.NoneExportedBefore || (!f.SkipDirective.Defined && !f.IncludeDirective.Defined) {
		return
	}

	var (
		ifNames             []string
		expressions         []string
		expressionVariables []string
	)
	if skipDirective := f.SkipDirective; skipDirective.Defined {
		if ifName := skipDirective.VariableName; len(ifName) > 0 {
			ifNames = append(ifNames, ifName)
		}
		if expression := skipDirective.Expression; len(expression) > 0 {
			if skipDirective.ExpressionIsVariable {
				expressionVariables = append(expressionVariables, expression)
			} else {
				expressions = append(expressions, expression)
			}
		}
	}
	if includeDirective := f.IncludeDirective; includeDirective.Defined {
		if ifName := includeDirective.VariableName; len(ifName) > 0 {
			ifNames = append(ifNames, ifName)
		}
		if expression := includeDirective.Expression; len(expression) > 0 {
			if includeDirective.ExpressionIsVariable {
				expressionVariables = append(expressionVariables, expression)
			} else {
				expressions = append(expressions, expression)
			}
		}
	}
	f.WaitExportedRequired = slices.ContainsFunc(exportedVariables, func(exported string) bool {
		return slices.Contains(ifNames, exported) || slices.ContainsFunc(expressions, func(expr string) bool {
			return strings.Contains(expr, fmt.Sprintf("arguments.%s", exported))
		})
	})
	if f.WaitExportedRequired || len(expressionVariables) == 0 {
		return
	}

	f.WaitExportedRequiredFunc = func(ctx *Context) bool {
		return slices.ContainsFunc(exportedVariables, func(exported string) bool {
			return slices.ContainsFunc(expressionVariables, func(expr string) bool {
				expr, _ = jsonparser.GetString(ctx.Variables, expr)
				return strings.Contains(expr, fmt.Sprintf("arguments.%s", exported))
			})
		})
	}
	return
}

func (f *Field) SetWaitExportedRequiredForArgument(arguments plan.ArgumentsConfigurations, exportedVariables []string) {
	if f.NoneExportedBefore || f.WaitExportedRequired {
		return
	}
	for _, item := range arguments {
		if item.SourceType == plan.FieldArgumentSource && slices.Contains(exportedVariables, item.Name) {
			f.WaitExportedRequired = true
			return
		}
	}
}

func (s *resultSet) renderInputTemplate(ctx *Context, fetch *SingleFetch, data []byte, preparedInput *fastbuffer.FastBuffer) error {
	inputTemplate := fetch.InputTemplate
	if skipFieldZeroValues, ok := s.skipBufferFieldZeroValues[fetch.BufferId]; ok && inputTemplate.ResetInputTemplateFunc != nil {
		skipFieldJsonPaths := make(map[string]bool, len(skipFieldZeroValues))
		for _, item := range skipFieldZeroValues {
			skipFieldJsonPaths[strings.Join(item.JsonPath, ".")] = true
		}
		inputTemplate = inputTemplate.ResetInputTemplateFunc(ctx, skipFieldJsonPaths)
	}
	return inputTemplate.Render(ctx, data, preparedInput)
}
