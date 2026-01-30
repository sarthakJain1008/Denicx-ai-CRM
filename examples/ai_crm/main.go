package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

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

	grp.POST("/apify/import", func(e *core.RequestEvent) error {
		res, err := importApifyDubaiEcommerceCSuite(e.App)
		if err != nil {
			e.App.Logger().Error("Apify import failed", "error", err)
			return e.InternalServerError("Failed to import from Apify.", err)
		}
		return e.JSON(http.StatusOK, res)
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

func ensureLeadsFieldsUpgrade(app core.App, col *core.Collection) error {
	changed := false
	if col.Fields.GetByName("job_title") == nil {
		col.Fields.Add(&core.TextField{Name: "job_title", Max: 255})
		changed = true
	}
	if col.Fields.GetByName("phone") == nil {
		col.Fields.Add(&core.TextField{Name: "phone", Max: 255})
		changed = true
	}
	if col.Fields.GetByName("linkedin") == nil {
		col.Fields.Add(&core.TextField{Name: "linkedin", Max: 1024})
		changed = true
	}
	if !changed {
		return nil
	}
	return app.Save(col)
}

func ensureLeadsCollection(app core.App) (*core.Collection, error) {
	if col, ok, err := findCollection(app, collectionLeads); err != nil {
		return nil, err
	} else if ok {
		if err := ensureLeadsFieldsUpgrade(app, col); err != nil {
			return nil, err
		}
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
		&core.TextField{Name: "job_title", Max: 255},
		&core.TextField{Name: "phone", Max: 255},
		&core.TextField{Name: "linkedin", Max: 1024},
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

type apifyLeadCandidate struct {
	FullName        string
	Email           string
	JobTitle        string
	Linkedin        string
	Phone           string
	CompanyName     string
	CompanyWebsite  string
	CompanyLinkedin string
}

func importApifyDubaiEcommerceCSuite(app core.App) (map[string]any, error) {
	token := strings.TrimSpace(os.Getenv("APIFY_TOKEN"))
	if token == "" {
		return nil, errors.New("missing APIFY_TOKEN")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 330*time.Second)
	defer cancel()

	endpoint := "https://api.apify.com/v2/acts/compass~crawler-google-places/run-sync-get-dataset-items"
	q := url.Values{}
	q.Set("token", token)
	q.Set("view", "leadsEnrichment")
	q.Set("clean", "true")
	apiURL := endpoint + "?" + q.Encode()

	input := map[string]any{
		"searchStringsArray":            []string{"e-commerce"},
		"locationQuery":                 "Dubai, United Arab Emirates",
		"countryCode":                   "AE",
		"language":                      "en",
		"maxCrawledPlacesPerSearch":     10,
		"maximumLeadsEnrichmentRecords": 3,
		"leadsEnrichmentDepartments":    []string{"c_suite"},
		"scrapeContacts":                false,
		"scrapePlaceDetailPage":         false,
	}

	payload, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, apiURL, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 330 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("apify request failed (%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	items, err := parseApifyItems(body)
	if err != nil {
		return nil, err
	}

	candidates := make([]apifyLeadCandidate, 0, len(items))
	for _, item := range items {
		candidates = append(candidates, extractApifyCandidates(item)...)
	}

	seen := map[string]struct{}{}
	deduped := make([]apifyLeadCandidate, 0, len(candidates))
	for _, c := range candidates {
		k := ""
		if strings.TrimSpace(c.Email) != "" {
			k = "email:" + strings.ToLower(strings.TrimSpace(c.Email))
		} else {
			k = "name_company:" + strings.ToLower(strings.TrimSpace(c.FullName)) + "|" + strings.ToLower(strings.TrimSpace(c.CompanyName))
		}
		if k == "name_company:|" {
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		deduped = append(deduped, c)
	}

	createdLeads := 0
	updatedLeads := 0
	skipped := 0

	for _, c := range deduped {
		if strings.TrimSpace(c.FullName) == "" || strings.TrimSpace(c.CompanyName) == "" {
			skipped++
			continue
		}

		acc, _, err := upsertAccountByName(app, c.CompanyName, c.CompanyWebsite)
		if err != nil {
			return nil, err
		}

		lead, created, err := upsertLead(app, acc.Id, c)
		if err != nil {
			return nil, err
		}
		_ = lead
		if created {
			createdLeads++
		} else {
			updatedLeads++
		}
	}

	return map[string]any{
		"createdLeads": createdLeads,
		"updatedLeads": updatedLeads,
		"skipped":      skipped,
		"total":        len(deduped),
	}, nil
}

func parseApifyItems(body []byte) ([]map[string]any, error) {
	var arr []map[string]any
	if err := json.Unmarshal(body, &arr); err == nil {
		return arr, nil
	}
	var wrapper struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(body, &wrapper); err != nil {
		return nil, err
	}
	return wrapper.Items, nil
}

func extractApifyCandidates(item map[string]any) []apifyLeadCandidate {
	if item == nil {
		return nil
	}

	if getString(item, "fullName") != "" || getString(item, "personId") != "" {
		return []apifyLeadCandidate{normalizeApifyLead(item)}
	}

	byIdx := map[int]map[string]any{}
	for k, v := range item {
		if strings.HasPrefix(k, "csuiteProfile_") {
			m, ok := byIdx[0]
			if !ok {
				m = map[string]any{}
				byIdx[0] = m
			}
			m[strings.TrimPrefix(k, "csuiteProfile_")] = v
			continue
		}
		if strings.HasPrefix(k, "csuiteProfile/") {
			rest := strings.TrimPrefix(k, "csuiteProfile/")
			parts := strings.SplitN(rest, "/", 2)
			if len(parts) != 2 {
				continue
			}
			idx, convErr := strconv.Atoi(parts[0])
			if convErr != nil {
				continue
			}
			m, ok := byIdx[idx]
			if !ok {
				m = map[string]any{}
				byIdx[idx] = m
			}
			m[parts[1]] = v
		}
	}

	if len(byIdx) == 0 {
		return nil
	}

	idxs := make([]int, 0, len(byIdx))
	for idx := range byIdx {
		idxs = append(idxs, idx)
	}
	sort.Ints(idxs)

	out := make([]apifyLeadCandidate, 0, len(idxs))
	for _, idx := range idxs {
		m := byIdx[idx]
		if m == nil {
			continue
		}
		out = append(out, normalizeApifyLead(m))
	}
	return out
}

func normalizeApifyLead(m map[string]any) apifyLeadCandidate {
	return apifyLeadCandidate{
		FullName:        firstNonEmpty(getString(m, "fullName"), strings.TrimSpace(getString(m, "firstName")+" "+getString(m, "lastName"))),
		Email:           getString(m, "email"),
		JobTitle:        firstNonEmpty(getString(m, "jobTitle"), getString(m, "headline")),
		Linkedin:        getString(m, "linkedinProfile"),
		Phone:           firstNonEmpty(getString(m, "mobileNumber"), getString(m, "phone")),
		CompanyName:     firstNonEmpty(getString(m, "companyName"), getString(m, "csuiteProfile_companyName")),
		CompanyWebsite:  getString(m, "companyWebsite"),
		CompanyLinkedin: getString(m, "companyLinkedin"),
	}
}

func upsertAccountByName(app core.App, companyName string, companyWebsite string) (*core.Record, bool, error) {
	companyName = strings.TrimSpace(companyName)
	if companyName == "" {
		return nil, false, errors.New("missing company name")
	}

	acc, err := app.FindFirstRecordByFilter(collectionAccounts, "name={:name}", dbx.Params{"name": companyName})
	if err == nil {
		return acc, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}

	accounts, err := app.FindCollectionByNameOrId(collectionAccounts)
	if err != nil {
		return nil, false, err
	}

	rec := core.NewRecord(accounts)
	rec.Set("name", companyName)
	if domain := domainFromWebsite(companyWebsite); domain != "" {
		rec.Set("domain", domain)
	}
	if err := app.Save(rec); err != nil {
		return nil, false, err
	}
	return rec, true, nil
}

func upsertLead(app core.App, accountId string, c apifyLeadCandidate) (*core.Record, bool, error) {
	leads, err := app.FindCollectionByNameOrId(collectionLeads)
	if err != nil {
		return nil, false, err
	}

	var lead *core.Record
	created := false

	if strings.TrimSpace(c.Email) != "" {
		lead, err = app.FindFirstRecordByFilter(collectionLeads, "email={:email}", dbx.Params{"email": strings.TrimSpace(c.Email)})
	} else {
		lead, err = app.FindFirstRecordByFilter(collectionLeads, "name={:name} && company={:company}", dbx.Params{"name": strings.TrimSpace(c.FullName), "company": strings.TrimSpace(c.CompanyName)})
	}

	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, false, err
		}
		lead = core.NewRecord(leads)
		created = true
		lead.Set("stage", "new")
		lead.Set("score", 0)
	}

	lead.Set("name", strings.TrimSpace(c.FullName))
	if strings.TrimSpace(c.Email) != "" {
		lead.Set("email", strings.TrimSpace(c.Email))
	}
	lead.Set("company", strings.TrimSpace(c.CompanyName))
	lead.Set("account", accountId)
	if strings.TrimSpace(c.JobTitle) != "" {
		lead.Set("job_title", strings.TrimSpace(c.JobTitle))
	}
	if strings.TrimSpace(c.Phone) != "" {
		lead.Set("phone", strings.TrimSpace(c.Phone))
	}
	if strings.TrimSpace(c.Linkedin) != "" {
		lead.Set("linkedin", strings.TrimSpace(c.Linkedin))
	}

	if err := app.Save(lead); err != nil {
		return nil, false, err
	}

	return lead, created, nil
}

func domainFromWebsite(site string) string {
	site = strings.TrimSpace(site)
	if site == "" {
		return ""
	}
	if !strings.Contains(site, "://") {
		site = "https://" + site
	}
	u, err := url.Parse(site)
	if err != nil {
		return ""
	}
	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	host = strings.TrimPrefix(host, "www.")
	return host
}

func getString(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	switch vv := v.(type) {
	case string:
		return strings.TrimSpace(vv)
	case float64:
		if vv == float64(int64(vv)) {
			return strconv.FormatInt(int64(vv), 10)
		}
		return strconv.FormatFloat(vv, 'f', -1, 64)
	case int:
		return strconv.Itoa(vv)
	case int64:
		return strconv.FormatInt(vv, 10)
	case json.Number:
		return vv.String()
	default:
		b, err := json.Marshal(vv)
		if err != nil {
			return ""
		}
		return strings.Trim(string(b), "\"")
	}
}

func firstNonEmpty(a, b string) string {
	a = strings.TrimSpace(a)
	if a != "" {
		return a
	}
	return strings.TrimSpace(b)
}
