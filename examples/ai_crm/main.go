package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/apis"
	"github.com/pocketbase/pocketbase/core"
	"github.com/pocketbase/pocketbase/tools/hook"
	"github.com/pocketbase/pocketbase/tools/types"
)

const (
	collectionAccounts   = "crm_accounts"
	collectionLeads      = "crm_leads"
	collectionDeals      = "crm_deals"
	collectionActivities = "crm_activities"
)

func main() {
	dataDir := resolveDataDir()
	log.Printf("ai_crm starting (dataDir=%s)", dataDir)
	app := pocketbase.NewWithConfig(pocketbase.Config{DefaultDataDir: dataDir})

	app.OnBootstrap().BindFunc(func(be *core.BootstrapEvent) error {
		if err := be.Next(); err != nil {
			return err
		}
		return ensureCRMSchema(be.App)
	})

	app.OnServe().Bind(&hook.Handler[*core.ServeEvent]{
		Priority: -10,
		Func: func(se *core.ServeEvent) error {
			bindAICRMRoutes(se)
			bindAICRMJobs(se)
			return se.Next()
		},
	})

	app.OnServe().Bind(&hook.Handler[*core.ServeEvent]{
		Priority: 999,
		Func: func(se *core.ServeEvent) error {
			publicDir := resolvePublicDir()
			if !se.Router.HasRoute(http.MethodGet, "/assets/denicx-logo.jpg") {
				se.Router.GET("/assets/denicx-logo.jpg", func(e *core.RequestEvent) error {
					http.ServeFile(e.Response, e.Request, resolveLogoPath())
					return nil
				})
			}
			if !se.Router.HasRoute(http.MethodGet, "/{path...}") {
				se.Router.GET("/{path...}", apis.Static(os.DirFS(publicDir), true))
			}
			return se.Next()
		},
	})

	if os.Getenv("VERCEL") != "" {
		// Prepend the "serve" command when running in Vercel
		// to ensure the web server starts automatically.
		os.Args = append([]string{os.Args[0], "serve"}, os.Args[1:]...)
	}

	if err := app.Start(); err != nil {
		log.Fatal(err)
	}
}

func resolveDataDir() string {
	wd, err := os.Getwd()
	if err != nil {
		return "./examples/ai_crm/pb_data"
	}

	wd = filepath.Clean(wd)
	if filepath.Base(wd) == "ai_crm" && filepath.Base(filepath.Dir(wd)) == "examples" {
		return "./pb_data"
	}

	return "./examples/ai_crm/pb_data"
}

func resolvePublicDir() string {
	wd, err := os.Getwd()
	if err != nil {
		return "./examples/ai_crm/pb_public"
	}

	wd = filepath.Clean(wd)
	if filepath.Base(wd) == "ai_crm" && filepath.Base(filepath.Dir(wd)) == "examples" {
		return "./pb_public"
	}

	return "./examples/ai_crm/pb_public"
}

func resolveLogoPath() string {
	wd, err := os.Getwd()
	if err != nil {
		return "./Denicx_Logo.jpg"
	}

	wd = filepath.Clean(wd)
	if filepath.Base(wd) == "ai_crm" && filepath.Base(filepath.Dir(wd)) == "examples" {
		return filepath.Join("..", "..", "Denicx_Logo.jpg")
	}

	return "./Denicx_Logo.jpg"
}

func bindAICRMRoutes(se *core.ServeEvent) {
	grp := se.Router.Group("/api/ai-crm")

	grp.GET("/health", func(e *core.RequestEvent) error {
		return e.JSON(http.StatusOK, map[string]any{"ok": true})
	})

	grp.POST("/seed", func(e *core.RequestEvent) error {
		count := 1
		if raw := e.Request.URL.Query().Get("count"); raw != "" {
			v, convErr := strconv.Atoi(raw)
			if convErr != nil {
				return e.BadRequestError("Invalid count.", convErr)
			}
			count = v
		}

		res, err := seedDemoData(e.App, count)
		if err != nil {
			return e.InternalServerError("Failed to seed demo data.", err)
		}
		return e.JSON(http.StatusOK, res)
	}).Bind(apis.RequireSuperuserAuth())

	grp.POST("/agents/run/{leadId}", func(e *core.RequestEvent) error {
		leadId := strings.TrimSpace(e.Request.PathValue("leadId"))
		if leadId == "" {
			return e.BadRequestError("Missing leadId.", nil)
		}

		result, err := runLeadAgent(e.App, leadId)
		if err != nil {
			return e.InternalServerError("Failed to run agent.", err)
		}

		return e.JSON(http.StatusOK, result)
	}).Bind(apis.RequireSuperuserAuth())
}

func bindAICRMJobs(se *core.ServeEvent) {
	go func() {
		_, _ = autoSeedUpTo(se.App, 25)
	}()

	se.App.Cron().MustAdd("aiCrmAutoPilot", "*/1 * * * *", func() {
		// fire-and-forget style job; keep it resilient
		_, _ = runAgentForPendingLeads(se.App, 5)
	})
}

func ensureCRMSchema(app core.App) error {
	if _, err := ensureAccountsCollection(app); err != nil {
		return err
	}
	if _, err := ensureLeadsCollection(app); err != nil {
		return err
	}
	if _, err := ensureDealsCollection(app); err != nil {
		return err
	}
	if _, err := ensureActivitiesCollection(app); err != nil {
		return err
	}
	return nil
}

func superuserOnlyRule() *string {
	return types.Pointer("@request.auth.collectionName = '_superusers'")
}

func ensureAccountsCollection(app core.App) (*core.Collection, error) {
	if col, ok, err := findCollection(app, collectionAccounts); err != nil {
		return nil, err
	} else if ok {
		return col, nil
	}

	col := core.NewBaseCollection(collectionAccounts)
	col.ListRule = superuserOnlyRule()
	col.ViewRule = superuserOnlyRule()
	col.CreateRule = superuserOnlyRule()
	col.UpdateRule = superuserOnlyRule()
	col.DeleteRule = superuserOnlyRule()

	col.Fields.Add(
		&core.TextField{Name: "name", Required: true, Presentable: true, Max: 255},
		&core.TextField{Name: "domain", Max: 255},
		&core.TextField{Name: "notes"},
		&core.AutodateField{Name: "created", OnCreate: true},
		&core.AutodateField{Name: "updated", OnCreate: true, OnUpdate: true},
	)

	if err := app.Save(col); err != nil {
		return nil, err
	}

	return col, nil
}

func ensureLeadsCollection(app core.App) (*core.Collection, error) {
	if col, ok, err := findCollection(app, collectionLeads); err != nil {
		return nil, err
	} else if ok {
		return col, nil
	}

	accounts, err := app.FindCollectionByNameOrId(collectionAccounts)
	if err != nil {
		return nil, err
	}

	col := core.NewBaseCollection(collectionLeads)
	col.ListRule = superuserOnlyRule()
	col.ViewRule = superuserOnlyRule()
	col.CreateRule = superuserOnlyRule()
	col.UpdateRule = superuserOnlyRule()
	col.DeleteRule = superuserOnlyRule()

	col.Fields.Add(
		&core.TextField{Name: "name", Required: true, Presentable: true, Max: 255},
		&core.EmailField{Name: "email"},
		&core.TextField{Name: "company", Max: 255},
		&core.RelationField{Name: "account", CollectionId: accounts.Id, MaxSelect: 1},
		&core.SelectField{Name: "stage", Required: true, Values: []string{"new", "outreached", "replied", "qualified", "proposal", "won", "lost"}},
		&core.NumberField{Name: "score", Min: floatPointer(0), Max: floatPointer(100)},
		&core.DateField{Name: "last_contacted"},
		&core.JSONField{Name: "agent_state"},
		&core.AutodateField{Name: "created", OnCreate: true},
		&core.AutodateField{Name: "updated", OnCreate: true, OnUpdate: true},
	)

	if err := app.Save(col); err != nil {
		return nil, err
	}

	return col, nil
}

func ensureDealsCollection(app core.App) (*core.Collection, error) {
	if col, ok, err := findCollection(app, collectionDeals); err != nil {
		return nil, err
	} else if ok {
		return col, nil
	}

	leads, err := app.FindCollectionByNameOrId(collectionLeads)
	if err != nil {
		return nil, err
	}

	col := core.NewBaseCollection(collectionDeals)
	col.ListRule = superuserOnlyRule()
	col.ViewRule = superuserOnlyRule()
	col.CreateRule = superuserOnlyRule()
	col.UpdateRule = superuserOnlyRule()
	col.DeleteRule = superuserOnlyRule()

	col.Fields.Add(
		&core.TextField{Name: "title", Required: true, Presentable: true, Max: 255},
		&core.RelationField{Name: "lead", CollectionId: leads.Id, MaxSelect: 1, Required: true},
		&core.SelectField{Name: "stage", Required: true, Values: []string{"qualification", "proposal", "negotiation", "won", "lost"}},
		&core.NumberField{Name: "amount"},
		&core.DateField{Name: "close_date"},
		&core.AutodateField{Name: "created", OnCreate: true},
		&core.AutodateField{Name: "updated", OnCreate: true, OnUpdate: true},
	)

	if err := app.Save(col); err != nil {
		return nil, err
	}

	return col, nil
}

func ensureActivitiesCollection(app core.App) (*core.Collection, error) {
	if col, ok, err := findCollection(app, collectionActivities); err != nil {
		return nil, err
	} else if ok {
		return col, nil
	}

	leads, err := app.FindCollectionByNameOrId(collectionLeads)
	if err != nil {
		return nil, err
	}
	deals, err := app.FindCollectionByNameOrId(collectionDeals)
	if err != nil {
		return nil, err
	}

	col := core.NewBaseCollection(collectionActivities)
	col.ListRule = superuserOnlyRule()
	col.ViewRule = superuserOnlyRule()
	col.CreateRule = superuserOnlyRule()
	col.UpdateRule = superuserOnlyRule()
	col.DeleteRule = superuserOnlyRule()

	col.Fields.Add(
		&core.SelectField{Name: "type", Required: true, Values: []string{"outreach_email", "outreach_call", "meeting", "note", "status_change"}},
		&core.RelationField{Name: "lead", CollectionId: leads.Id, MaxSelect: 1, Required: true},
		&core.RelationField{Name: "deal", CollectionId: deals.Id, MaxSelect: 1},
		&core.TextField{Name: "content", Max: 5000},
		&core.JSONField{Name: "metadata"},
		&core.AutodateField{Name: "created", OnCreate: true},
		&core.AutodateField{Name: "updated", OnCreate: true, OnUpdate: true},
	)

	if err := app.Save(col); err != nil {
		return nil, err
	}

	return col, nil
}

func findCollection(app core.App, nameOrId string) (*core.Collection, bool, error) {
	col, err := app.FindCollectionByNameOrId(nameOrId)
	if err == nil {
		return col, true, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	return nil, false, err
}

func floatPointer(v float64) *float64 {
	return &v
}

func autoSeedUpTo(app core.App, target int) (int, error) {
	if target <= 0 {
		return 0, nil
	}
	if target > 500 {
		target = 500
	}

	total, err := app.CountRecords(collectionLeads)
	if err != nil {
		return 0, err
	}

	missing := target - int(total)
	if missing <= 0 {
		return 0, nil
	}

	_, err = seedDemoData(app, missing)
	if err != nil {
		return 0, err
	}

	return missing, nil
}

func seedDemoData(app core.App, count int) (map[string]any, error) {
	if count <= 0 {
		count = 1
	}
	if count > 500 {
		count = 500
	}

	accounts, err := app.FindCollectionByNameOrId(collectionAccounts)
	if err != nil {
		return nil, err
	}
	leads, err := app.FindCollectionByNameOrId(collectionLeads)
	if err != nil {
		return nil, err
	}
	deals, err := app.FindCollectionByNameOrId(collectionDeals)
	if err != nil {
		return nil, err
	}

	return seedDemoDataWithCollections(app, accounts, leads, deals, count)
}

func seedDemoDataWithCollections(app core.App, accounts *core.Collection, leads *core.Collection, deals *core.Collection, count int) (map[string]any, error) {
	firstNames := []string{"Taylor", "Jordan", "Casey", "Riley", "Avery", "Sam", "Jamie", "Morgan", "Alex", "Quinn"}
	lastNames := []string{"Shah", "Patel", "Singh", "Kim", "Chen", "Garcia", "Brown", "Smith", "Khan", "Ng"}
	companies := []string{"Acme", "Globex", "Initech", "Umbrella", "Stark", "Wayne", "Wonka", "Hooli", "Vehement", "Soylent"}
	domains := []string{"example.com", "acme.test", "company.test", "demo.local", "corp.test"}

	leadIds := make([]string, 0, count)
	accountIds := make([]string, 0, count)
	dealIds := make([]string, 0, count)

	leadStages := []string{"new", "outreached", "replied", "qualified", "proposal"}
	dealStagesByLead := map[string]string{
		"new":        "qualification",
		"outreached": "qualification",
		"replied":    "qualification",
		"qualified":  "qualification",
		"proposal":   "proposal",
		"won":        "won",
		"lost":       "lost",
	}

	for i := 0; i < count; i++ {
		fn := firstNames[rand.IntN(len(firstNames))]
		ln := lastNames[rand.IntN(len(lastNames))]
		company := companies[rand.IntN(len(companies))] + " " + []string{"Labs", "Systems", "AI", "Holdings", "Tech"}[rand.IntN(5)]
		domain := strings.ToLower(strings.ReplaceAll(company, " ", "")) + "." + domains[rand.IntN(len(domains))]
		name := fn + " " + ln
		email := strings.ToLower(fn+"."+ln) + "+" + strconv.Itoa(rand.IntN(100000)) + "@" + domains[rand.IntN(len(domains))]
		stage := leadStages[rand.IntN(len(leadStages))]
		score := rand.IntN(101)

		acc := core.NewRecord(accounts)
		acc.Set("name", company)
		acc.Set("domain", domain)
		if err := app.Save(acc); err != nil {
			return nil, err
		}

		lead := core.NewRecord(leads)
		lead.Set("name", name)
		lead.Set("email", email)
		lead.Set("company", company)
		lead.Set("account", acc.Id)
		lead.Set("stage", stage)
		lead.Set("score", score)
		if err := app.Save(lead); err != nil {
			return nil, err
		}

		deal := core.NewRecord(deals)
		deal.Set("title", company+" / Starter")
		deal.Set("lead", lead.Id)
		deal.Set("stage", dealStagesByLead[stage])
		deal.Set("amount", 1000+rand.IntN(50000))
		if err := app.Save(deal); err != nil {
			return nil, err
		}

		accountIds = append(accountIds, acc.Id)
		leadIds = append(leadIds, lead.Id)
		dealIds = append(dealIds, deal.Id)
	}

	return map[string]any{
		"count":      count,
		"accountIds": accountIds,
		"leadIds":    leadIds,
		"dealIds":    dealIds,
	}, nil
}

type agentRunResult struct {
	LeadId      string         `json:"leadId"`
	OldStage    string         `json:"oldStage"`
	NewStage    string         `json:"newStage"`
	Action      string         `json:"action"`
	Message     string         `json:"message"`
	ActivityId  string         `json:"activityId"`
	DealCreated bool           `json:"dealCreated"`
	Meta        map[string]any `json:"meta"`
}

func runLeadAgent(app core.App, leadId string) (*agentRunResult, error) {
	lead, err := app.FindRecordById(collectionLeads, leadId)
	if err != nil {
		return nil, err
	}

	oldStage := lead.GetString("stage")
	if oldStage == "" {
		oldStage = "new"
	}

	if oldStage == "won" || oldStage == "lost" {
		return &agentRunResult{
			LeadId:   leadId,
			OldStage: oldStage,
			NewStage: oldStage,
			Action:   "noop",
			Message:  "Lead already finalized.",
			Meta:     map[string]any{"final": true},
		}, nil
	}

	action, message, newStage, activityType := planNextStep(lead, oldStage)

	// ensure deal exists once qualified
	dealCreated := false
	if newStage == "qualified" || newStage == "proposal" || newStage == "won" {
		created, err := ensureDealForLead(app, lead)
		if err != nil {
			return nil, err
		}
		dealCreated = created
	}

	activityId, err := createActivity(app, lead, activityType, message, map[string]any{
		"agentAction": action,
		"fromStage":   oldStage,
		"toStage":     newStage,
	})
	if err != nil {
		return nil, err
	}

	lead.Set("stage", newStage)
	lead.Set("agent_state", map[string]any{
		"last_action":  action,
		"last_message": message,
		"old_stage":    oldStage,
		"new_stage":    newStage,
	})
	if err := app.Save(lead); err != nil {
		return nil, err
	}

	// if lead is won, mark deal as won too
	if newStage == "won" {
		_ = markDealStage(app, lead.Id, "won")
	}

	return &agentRunResult{
		LeadId:      lead.Id,
		OldStage:    oldStage,
		NewStage:    newStage,
		Action:      action,
		Message:     message,
		ActivityId:  activityId,
		DealCreated: dealCreated,
		Meta:        map[string]any{"activityType": activityType},
	}, nil
}

func runAgentForPendingLeads(app core.App, limit int) (int, error) {
	if limit <= 0 {
		limit = 5
	}

	leads, err := app.FindRecordsByFilter(
		collectionLeads,
		"stage != 'won' && stage != 'lost'",
		"-updated",
		limit,
		0,
	)
	if err != nil {
		return 0, err
	}

	processed := 0
	for _, lead := range leads {
		_, err := runLeadAgent(app, lead.Id)
		if err != nil {
			app.Logger().Warn("ai_crm agent run failed", "leadId", lead.Id, "error", err)
			continue
		}
		processed++
	}

	return processed, nil
}

func planNextStep(lead *core.Record, stage string) (action string, message string, newStage string, activityType string) {
	name := lead.GetString("name")
	company := lead.GetString("company")

	switch stage {
	case "new":
		return "draft_outreach", fmt.Sprintf("Hi %s,\n\nI noticed %s and thought it might be worth a quick chat. Are you open to a 15-min call this week?\n\nBest,\nYou", safe(name), safe(company)), "outreached", "outreach_email"
	case "outreached":
		return "follow_up", fmt.Sprintf("Follow up with %s at %s. Ask 2-3 qualifying questions and propose next step.", safe(name), safe(company)), "qualified", "note"
	case "replied":
		return "qualify", fmt.Sprintf("%s replied. Capture pain points, budget, timeline and move to qualified.", safe(name)), "qualified", "note"
	case "qualified":
		return "proposal", fmt.Sprintf("Create a proposal for %s (%s) and send it.", safe(name), safe(company)), "proposal", "note"
	case "proposal":
		return "close", fmt.Sprintf("If no blockers, move %s to won and log the reason.", safe(name)), "won", "status_change"
	default:
		return "noop", "No action planned.", stage, "note"
	}
}

func createActivity(app core.App, lead *core.Record, typ string, content string, metadata map[string]any) (string, error) {
	acts, err := app.FindCollectionByNameOrId(collectionActivities)
	if err != nil {
		return "", err
	}

	rec := core.NewRecord(acts)
	rec.Set("type", typ)
	rec.Set("lead", lead.Id)
	rec.Set("content", content)
	rec.Set("metadata", metadata)

	if err := app.Save(rec); err != nil {
		return "", err
	}

	return rec.Id, nil
}

func ensureDealForLead(app core.App, lead *core.Record) (bool, error) {
	_, err := app.FindFirstRecordByFilter(collectionDeals, "lead={:lead}", dbx.Params{"lead": lead.Id})
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}

	deals, err := app.FindCollectionByNameOrId(collectionDeals)
	if err != nil {
		return false, err
	}

	rec := core.NewRecord(deals)
	rec.Set("title", fmt.Sprintf("%s / New deal", safe(lead.GetString("company"))))
	rec.Set("lead", lead.Id)
	rec.Set("stage", "qualification")
	if err := app.Save(rec); err != nil {
		return false, err
	}

	return true, nil
}

func markDealStage(app core.App, leadId string, stage string) error {
	deal, err := app.FindFirstRecordByFilter(collectionDeals, "lead={:lead}", dbx.Params{"lead": leadId})
	if err != nil {
		return err
	}
	deal.Set("stage", stage)
	return app.Save(deal)
}

func safe(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "there"
	}
	return s
}
