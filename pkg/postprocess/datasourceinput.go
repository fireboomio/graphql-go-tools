package postprocess

import (
	"strconv"
	"strings"

	"github.com/wundergraph/graphql-go-tools/pkg/engine/plan"
	"github.com/wundergraph/graphql-go-tools/pkg/engine/resolve"
)

type ProcessDataSource struct{}

func (d *ProcessDataSource) Process(pre plan.Plan) plan.Plan {
	switch t := pre.(type) {
	case *plan.SynchronousResponsePlan:
		d.traverseNode(t.Response.Data)
	case *plan.StreamingResponsePlan:
		d.traverseNode(t.Response.InitialResponse.Data)
		for i := range t.Response.Patches {
			d.traverseFetch(t.Response.Patches[i].Fetch)
			d.traverseNode(t.Response.Patches[i].Value)
		}
	case *plan.SubscriptionResponsePlan:
		d.traverseTrigger(&t.Response.Trigger)
		d.traverseNode(t.Response.Response.Data)
	}
	return pre
}

func (d *ProcessDataSource) traverseNode(node resolve.Node) {
	switch n := node.(type) {
	case *resolve.Object:
		d.traverseFetch(n.Fetch)
		for i := range n.Fields {
			d.traverseNode(n.Fields[i].Value)
		}
	case *resolve.Array:
		d.traverseNode(n.Item)
	}
}

func (d *ProcessDataSource) traverseFetch(fetch resolve.Fetch) {
	if fetch == nil {
		return
	}
	switch f := fetch.(type) {
	case *resolve.SingleFetch:
		d.traverseSingleFetch(f)
	case *resolve.BatchFetch:
		d.traverseSingleFetch(f.Fetch)
	case *resolve.ParallelFetch:
		for i := range f.Fetches {
			d.traverseFetch(f.Fetches[i])
		}
	}
}

func (d *ProcessDataSource) traverseTrigger(trigger *resolve.GraphQLSubscriptionTrigger) {
	d.resolveInputTemplate(trigger.Variables, string(trigger.Input), &trigger.InputTemplate, nil)
	trigger.Input = nil
	trigger.Variables = nil
}

func (d *ProcessDataSource) traverseSingleFetch(fetch *resolve.SingleFetch) {
	d.setResetInputTemplateFunc(fetch)
	d.resolveInputTemplate(fetch.Variables, fetch.Input, &fetch.InputTemplate, fetch.SkipVariableFuncs)
	fetch.Input = ""
	fetch.Variables = nil
	fetch.InputTemplate.SetTemplateOutputToNullOnVariableNull = fetch.SetTemplateOutputToNullOnVariableNull
	fetch.SetTemplateOutputToNullOnVariableNull = false
	fetch.InputTemplate.RewriteVariableFunc = fetch.RewriteVariableFunc
	fetch.RewriteVariableFunc = nil
}

func (d *ProcessDataSource) resolveInputTemplate(variables resolve.Variables, input string, template *resolve.InputTemplate,
	skipVariableFuncs map[string][]func(*resolve.Context) bool) {
	if input == "" {
		return
	}

	if !strings.Contains(input, "$$") {
		template.AddStaticTemplateSegment(input)
		return
	}

	isVariable := false
	segments := strings.Split(input, "$$")
	for _, segment := range segments {
		switch {
		case isVariable:
			i, _ := strconv.Atoi(segment)
			variableSegment := (variables)[i].TemplateSegment()
			if skipFuncs, ok := skipVariableFuncs[variableSegment.VariableSourcePath[0]]; ok {
				variableSegment.VariableSkipFuncs = skipFuncs
			}
			template.AddTemplateSegment(variableSegment)
			isVariable = false
		default:
			template.AddStaticTemplateSegment(segment)
			isVariable = true
		}
	}
}

func (d *ProcessDataSource) setResetInputTemplateFunc(fetch *resolve.SingleFetch) {
	resetInputFunc := fetch.ResetInputFunc
	if resetInputFunc == nil {
		return
	}

	variables := fetch.Variables
	fetch.InputTemplate.ResetInputTemplateFunc = func(ctx *resolve.Context, data map[string]bool) resolve.InputTemplate {
		inputTemplate := resolve.InputTemplate{}
		inputTemplate.SetTemplateOutputToNullOnVariableNull = fetch.InputTemplate.SetTemplateOutputToNullOnVariableNull
		d.resolveInputTemplate(variables, resetInputFunc(ctx, data), &inputTemplate, fetch.SkipVariableFuncs)
		if len(inputTemplate.Segments) == 0 {
			inputTemplate = fetch.InputTemplate
		}
		return inputTemplate
	}
	fetch.ResetInputFunc = nil
}
