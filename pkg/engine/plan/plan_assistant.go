package plan

import (
	"encoding/json"
	"github.com/buger/jsonparser"
	"github.com/wundergraph/graphql-go-tools/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/resolve"
	"github.com/wundergraph/graphql-go-tools/pkg/lexer/literal"
	"golang.org/x/exp/slices"
)

func (v *Visitor) setWaitExportedRequiredForArguments(config objectFetchConfiguration) {
	objectPopField := config.objectPopField
	if objectPopField == nil || objectPopField.WaitExportedRequired || objectPopField.LengthOfExportedBefore == 0 {
		return
	}
	for _, item := range config.object.Fields {
		if item.WaitExportedRequired {
			objectPopField.WaitExportedRequired = true
			return
		}
	}

	var variables []resolve.Variable
	switch f := config.object.Fetch.(type) {
	case *resolve.SingleFetch:
		variables = f.Variables
	case *resolve.BatchFetch:
		variables = f.Fetch.Variables
	default:
		return
	}
	for _, item := range variables {
		if item.GetVariableKind() != resolve.ContextVariableKind {
			continue
		}
		segment := item.TemplateSegment()
		if segment.VariableGenerated {
			continue
		}
		for variable, index := range v.exportedVariables {
			if variable == segment.VariableSourcePath[0] && index < objectPopField.LengthOfExportedBefore {
				objectPopField.WaitExportedRequired = true
				return
			}
		}
	}
}

func (v *Visitor) resetWaitExportedRequired(ref int) {
	index, ok := v.currentFieldIndexes[ref]
	if !ok || index >= len(v.currentFieldIndexes) {
		return
	}

	currentField := v.currentFields[index].popField
	if currentField.LengthOfExportedBefore == 0 || currentField.WaitExportedRequired {
		return
	}

	var itemWaitFuncs []func(*resolve.Context) bool
	for _, item := range *v.currentFields[index].fields {
		if item.WaitExportedRequired {
			currentField.WaitExportedRequired = true
			return
		}
		if item.WaitExportedRequiredFunc != nil {
			itemWaitFuncs = append(itemWaitFuncs, item.WaitExportedRequiredFunc)
		}
	}
	if len(itemWaitFuncs) == 0 {
		return
	}

	currentWaitFunc := currentField.WaitExportedRequiredFunc
	currentField.WaitExportedRequiredFunc = func(ctx *resolve.Context) bool {
		return currentWaitFunc != nil && currentWaitFunc(ctx) ||
			slices.ContainsFunc(itemWaitFuncs, func(f func(*resolve.Context) bool) bool { return f(ctx) })
	}
	return
}

const (
	formatDateTimeDirective       = "formatDateTime"
	formatDateTimeArgFormat       = "format"
	formatDateTimeArgCustomFormat = "customFormat"
)

func (v *Visitor) resolveDateFormatArguments(fieldRef int) map[string]string {
	if !v.Operation.Fields[fieldRef].HasDirectives {
		return nil
	}

	for _, ref := range v.Operation.Fields[fieldRef].Directives.Refs {
		if v.Operation.Input.ByteSliceString(v.Operation.Directives[ref].Name) != formatDateTimeDirective {
			continue
		}

		arguments := make(map[string]string, 2)
		formatValue, ok := v.Operation.DirectiveArgumentValueByName(ref, []byte(formatDateTimeArgFormat))
		if ok && formatValue.Kind == ast.ValueKindEnum {
			arguments[formatDateTimeArgFormat] = v.Operation.EnumValueNameString(formatValue.Ref)
		}
		customFormatValue, ok := v.Operation.DirectiveArgumentValueByName(ref, []byte(formatDateTimeArgCustomFormat))
		if ok && customFormatValue.Kind == ast.ValueKindString {
			arguments[formatDateTimeArgCustomFormat] = v.Operation.StringValueContentString(customFormatValue.Ref)
		}
		return arguments
	}
	return nil
}

func (v *Visitor) resolveSkipVariables(ref int) []resolve.SkipVariableDirective {
	var field []resolve.SkipVariableDirective
	for _, i := range v.Operation.Fields[ref].Directives.Refs {
		if v.Operation.DirectiveNameString(i) != "skipVariable" {
			continue
		}
		itemDirective := resolve.SkipVariableDirective{}
		if value, ok := v.Operation.DirectiveArgumentValueByName(i, []byte(`variables`)); ok {
			valueBytes, _ := v.Operation.ValueToJSON(value)
			_ = json.Unmarshal(valueBytes, &itemDirective.VariableNames)
		}
		if value, ok := v.Operation.DirectiveArgumentValueByName(i, literal.IfRule); ok {
			itemDirective.ExpressionIsVariable = value.Kind == ast.ValueKindVariable
			if itemDirective.ExpressionIsVariable {
				itemDirective.Expression = v.Operation.VariableValueNameString(value.Ref)
			} else {
				itemDirective.Expression = v.Operation.ValueContentString(value)
			}
		}
		field = append(field, itemDirective)
	}
	return field
}

func (v *Visitor) containsFirstRawResult(ref int) bool {
	for _, i := range v.Operation.Fields[ref].Directives.Refs {
		if v.Operation.DirectiveNameString(i) == "firstRawResult" {
			return true
		}
	}
	return false
}

func (v *Visitor) setSkipVariableFuncForFetch(internal objectFetchConfiguration, fetch *resolve.SingleFetch) {
	if internal.object == nil || len(internal.object.Fields) == 0 {
		return
	}

	fetch.SkipVariableFuncs = make(map[string][]func(*resolve.Context) bool)
	for _, field := range internal.object.Fields {
		for _, skipVariableDirective := range field.SkipVariableDirectives {
			skipVariableFunc := func(ctx *resolve.Context) bool {
				expression := skipVariableDirective.Expression
				if skipVariableDirective.ExpressionIsVariable {
					expression, _ = jsonparser.GetString(ctx.Variables, expression)
				}
				return ctx.RuleEvaluate(ctx.Variables, expression)
			}
			for _, variable := range skipVariableDirective.VariableNames {
				fetch.SkipVariableFuncs[variable] = append(fetch.SkipVariableFuncs[variable], skipVariableFunc)
			}
		}
	}

}
