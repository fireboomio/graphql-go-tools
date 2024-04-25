package resolve

import (
	"fmt"
	"github.com/buger/jsonparser"
	"golang.org/x/exp/slices"
	"strings"
)

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
		if skipRequired = !slices.Contains(skipEffective, false); skipRequired {
			return
		}
	}
	return
}

func (f *Field) SetWaitExportedRequired(exportedVariables []string) {
	if len(exportedVariables) == 0 || (!f.SkipDirective.Defined && !f.IncludeDirective.Defined) {
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
