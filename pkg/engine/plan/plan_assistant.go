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
	if objectPopField == nil {
		parallelFetch, ok := config.object.Fetch.(*resolve.ParallelFetch)
		if !ok {
			return
		}
		lastFetch := v.getLastFromParallelFetch(parallelFetch)
		var lastField *resolve.Field
		for _, item := range config.object.Fields {
			if lastFetch.BufferId == item.BufferID {
				lastField = item
				break
			}
		}
		if lastField == nil || lastField.WaitExportedRequired {
			return
		}
		v.matchExportVariables(lastFetch.Variables, func(string, int) bool {
			lastField.WaitExportedRequired = true
			return true
		})
		return
	}

	if objectPopField.WaitExportedRequired || objectPopField.LengthOfExportedBefore == 0 {
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
	case *resolve.ParallelFetch:
		variables = v.getLastFromParallelFetch(f).Variables
	}
	v.matchExportVariables(variables, func(_ string, index int) bool {
		matched := index < objectPopField.LengthOfExportedBefore
		if matched {
			objectPopField.WaitExportedRequired = true
		}
		return matched
	})
}

func (v *Visitor) matchExportVariables(variables resolve.Variables, match func(string, int) bool) {
	for _, item := range variables {
		ctxVariable, ok := item.(*resolve.ContextVariable)
		if !ok || ctxVariable.Generated {
			continue
		}
		for variable, export := range v.exportedVariables {
			if variable == ctxVariable.Path[0] && match(variable, export.Index) {
				return
			}
		}
	}
}

func (v *Visitor) getLastFromParallelFetch(parallelFetch *resolve.ParallelFetch) (fetch *resolve.SingleFetch) {
	switch ff := parallelFetch.Fetches[len(parallelFetch.Fetches)-1].(type) {
	case *resolve.SingleFetch:
		fetch = ff
	case *resolve.BatchFetch:
		fetch = ff.Fetch
	}
	return
}

func (v *Visitor) resetWaitExportedRequired(ref int) {
	index, ok := v.currentFieldIndexes[ref]
	if !ok || index >= len(v.currentFields) {
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

const asyncResolveDirective = "asyncResolve"

func (v *Visitor) resolveAsyncResolve(fieldRef int) bool {
	if !v.Operation.Fields[fieldRef].HasDirectives {
		return false
	}

	for _, ref := range v.Operation.Fields[fieldRef].Directives.Refs {
		if v.Operation.Input.ByteSliceString(v.Operation.Directives[ref].Name) == asyncResolveDirective {
			return true
		}
	}
	return false
}

func (v *Visitor) resolveSkipVariables(ref int) []resolve.SkipVariableDirective {
	var directives []resolve.SkipVariableDirective
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
		directives = append(directives, itemDirective)
	}
	return directives
}

func (v *Visitor) containsFirstRawResult(ref int) bool {
	for _, i := range v.Operation.Fields[ref].Directives.Refs {
		if v.Operation.DirectiveNameString(i) == "firstRawResult" {
			return true
		}
	}
	return false
}

func (v *Visitor) containsPrismaAliased(ref int) bool {
	for _, i := range v.Operation.Fields[ref].Directives.Refs {
		if v.Operation.DirectiveNameString(i) == "prismaAliased" {
			return true
		}
	}
	return false
}

func (v *Visitor) FieldHasConfig(ref int) bool {
	_, ok := v.fieldConfigs[ref]
	return ok
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
