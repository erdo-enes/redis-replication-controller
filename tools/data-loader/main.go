// Command data-loader populates a Redis instance with at least 10 GB of
// realistic application data, using pipelined writes for throughput. It is
// meant to be run as a one-shot Kubernetes Job before failover testing so the
// failover-probe can observe a realistic data volume.
//
// Data shapes written (mimic a typical web application):
//   session:<uuid>              – user session (JSON, ~3 KB)
//   user:<id>:profile           – user profile  (JSON, ~2 KB)
//   cache:product:<id>          – product detail (JSON, ~4 KB)
//   cache:category:<slug>       – category page  (JSON, ~5 KB)
//   cache:api:<path_hash>       – API response   (JSON, ~6 KB)
//   ratelimit:<ip>:<window>     – rate-limit counter (int)
//   feature:<name>              – feature flag   (JSON, ~1 KB)
//   lb:sticky:<session_id>      – load-balancer sticky mapping (string)
//
// No external dependencies – uses the same minimal RESP client as
// failover-probe.
package main

import (
	"bufio"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
)

var rnd = mustRand()

type config struct {
	addr        string
	dialTimeout time.Duration
	cmdTimeout  time.Duration
	targetGB    int
	batchSize   int
	keyPrefix   string
}

func loadConfig() config {
	return config{
		addr:        env("TARGET_ADDR", "redis-write-cache.redis-test-enes.svc.cluster.local:6379"),
		dialTimeout: envDur("DIAL_TIMEOUT", 5*time.Second),
		cmdTimeout:  envDur("CMD_TIMEOUT", 5*time.Second),
		targetGB:    envInt("TARGET_GB", 10),
		batchSize:   envInt("BATCH_SIZE", 500),
		keyPrefix:   env("KEY_PREFIX", "ld"),
	}
}

type generator struct {
	firstNames  []string
	lastNames   []string
	domains     []string
	userAgents  []string
	countries   []string
	categories  []category
	products    []productTemplate
	apiPaths    []string
	featureFlags []featureTemplate
	ips         []string
}

type category struct {
	Slug, Name, Description string
}

type productTemplate struct {
	Name, Brand, Category string
}

type featureTemplate struct {
	Name, Description string
}

func newGenerator() *generator {
	return &generator{
		firstNames: []string{"Emma", "Liam", "Olivia", "Noah", "Ava", "Oliver", "Sophia", "Elijah",
			"Isabella", "James", "Mia", "William", "Charlotte", "Benjamin", "Amelia", "Lucas",
			"Harper", "Henry", "Evelyn", "Alexander", "Abigail", "Daniel", "Emily", "Michael"},
		lastNames: []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
			"Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson",
			"Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"},
		domains: []string{"gmail.com", "yahoo.com", "outlook.com", "proton.me", "icloud.com",
			"company.org", "startup.io", "enterprise.co"},
		userAgents: []string{
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
			"Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5) AppleWebKit/605.1.15 Safari/605.1.15",
			"Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
			"Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
			"Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 Chrome/125.0.6422.165 Mobile Safari/537.36",
		},
		countries: []string{"US", "DE", "GB", "FR", "JP", "BR", "IN", "CA", "AU", "NL", "SE", "SG"},
		categories: []category{
			{Slug: "electronics", Name: "Electronics", Description: "Phones, laptops, tablets and accessories"},
			{Slug: "clothing", Name: "Clothing & Apparel", Description: "Men's and women's fashion, shoes and accessories"},
			{Slug: "home-garden", Name: "Home & Garden", Description: "Furniture, decor, kitchen appliances and tools"},
			{Slug: "books", Name: "Books & Media", Description: "Fiction, non-fiction, e-books and audiobooks"},
			{Slug: "sports", Name: "Sports & Outdoors", Description: "Equipment, apparel and nutrition for every sport"},
			{Slug: "health-beauty", Name: "Health & Beauty", Description: "Skincare, makeup, vitamins and personal care"},
			{Slug: "toys", Name: "Toys & Games", Description: "Board games, action figures, puzzles and educational toys"},
			{Slug: "automotive", Name: "Automotive", Description: "Parts, tools, accessories and car care products"},
		},
		products: []productTemplate{
			{Name: "Wireless Noise-Cancelling Headphones", Brand: "SoundMax", Category: "electronics"},
			{Name: "4K Ultra HD Smart TV 55\"", Brand: "VisioTech", Category: "electronics"},
			{Name: "Mechanical Keyboard RGB", Brand: "KeyCraft", Category: "electronics"},
			{Name: "Slim Fit Oxford Shirt", Brand: "UrbanThreads", Category: "clothing"},
			{Name: "Waterproof Hiking Boots", Brand: "TrailBlazer", Category: "clothing"},
			{Name: "Ergonomic Office Chair", Brand: "ErgoPlus", Category: "home-garden"},
			{Name: "Stainless Steel French Press", Brand: "BrewMaster", Category: "home-garden"},
			{Name: "The Silent Algorithm (Hardcover)", Brand: "Penwick Press", Category: "books"},
			{Name: "Carbon Fibre Tennis Racket", Brand: "AcePro", Category: "sports"},
			{Name: "Vitamin D3 + K2 Complex", Brand: "VitaBloom", Category: "health-beauty"},
			{Name: "Strategy Board Game: Empire Builders", Brand: "GameForge", Category: "toys"},
			{Name: "Synthetic Motor Oil 5W-30", Brand: "LubeTech", Category: "automotive"},
		},
		apiPaths: []string{
			"/api/v1/products?category=electronics&page=1&limit=50",
			"/api/v1/products?category=clothing&sort=price_asc&page=2",
			"/api/v1/users/me/orders?status=delivered&from=2024-01-01",
			"/api/v1/search?q=wireless+headphones&filters=brand:SoundMax,price:50-200",
			"/api/v1/recommendations?user_id=%s&strategy=collaborative",
			"/api/v1/inventory/check?sku=%s&warehouse=eu-central-1",
			"/api/v1/cart/%s?include=items,summary,promotions",
			"/api/v1/reviews?product_id=%s&sort=helpful&limit=10",
		},
		featureFlags: []featureTemplate{
			{Name: "dark-mode-v2", Description: "Next-gen dark mode with adaptive contrast and scheduled switching"},
			{Name: "checkout-redesign", Description: "New single-page checkout flow with address autocomplete"},
			{Name: "ai-recommendations", Description: "ML-powered product recommendations on homepage and PDP"},
			{Name: "real-time-notifications", Description: "WebSocket push notifications for order status and price drops"},
			{Name: "loyalty-points-2.0", Description: "Revamped loyalty program with tiered multipliers and partner redemption"},
		},
		ips: makeIPs(120),
	}
}

func makeIPs(n int) []string {
	ips := make([]string, n)
	for i := range ips {
		ips[i] = fmt.Sprintf("%d.%d.%d.%d", 10+rndN(215), rndN(256), rndN(256), 1+rndN(254))
	}
	return ips
}

// ----- data shapes -------------------------------------------------------------

type sessionData struct {
	UserID    string `json:"user_id"`
	CreatedAt string `json:"created_at"`
	ExpiresAt string `json:"expires_at"`
	UserAgent string `json:"user_agent"`
	IP        string `json:"ip"`
	Country   string `json:"country"`
	DeviceID  string `json:"device_id"`
}

type userProfile struct {
	UserID      string            `json:"user_id"`
	FirstName   string            `json:"first_name"`
	LastName    string            `json:"last_name"`
	Email       string            `json:"email"`
	Country     string            `json:"country"`
	CreatedAt   string            `json:"created_at"`
	LastLoginAt string            `json:"last_login_at"`
	Tier        string            `json:"tier"`
	Preferences map[string]bool   `json:"preferences"`
	Addresses   []userAddress     `json:"addresses"`
}

type userAddress struct {
	Type    string `json:"type"`
	Street  string `json:"street"`
	City    string `json:"city"`
	Zip     string `json:"zip"`
	Country string `json:"country"`
}

type productDetail struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Brand       string            `json:"brand"`
	Category    string            `json:"category"`
	Price       float64           `json:"price"`
	Currency    string            `json:"currency"`
	InStock     bool              `json:"in_stock"`
	Rating      float64           `json:"rating"`
	ReviewCount int               `json:"review_count"`
	Variants    []productVariant  `json:"variants"`
	Attributes  map[string]string `json:"attributes"`
	ImageURLs   []string          `json:"image_urls"`
}

type productVariant struct {
	SKU   string  `json:"sku"`
	Size  string  `json:"size"`
	Color string  `json:"color"`
	Price float64 `json:"price"`
	Stock int     `json:"stock"`
}

type categoryPage struct {
	Slug        string          `json:"slug"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	ProductIDs  []string        `json:"product_ids"`
	TotalCount  int             `json:"total_count"`
	Page        int             `json:"page"`
	PageSize    int             `json:"page_size"`
	Facets      []categoryFacet `json:"facets"`
	SortOption  string          `json:"sort_option"`
}

type categoryFacet struct {
	Key    string            `json:"key"`
	Values []categoryFacetVal `json:"values"`
}

type categoryFacetVal struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

type apiResponse struct {
	Data       interface{} `json:"data"`
	Meta       apiMeta     `json:"meta"`
	RequestID  string      `json:"request_id"`
	ServerTime string      `json:"server_time"`
}

type apiMeta struct {
	Page       int    `json:"page"`
	PageSize   int    `json:"page_size"`
	TotalCount int    `json:"total_count"`
	TotalPages int    `json:"total_pages"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type featureFlag struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Enabled     bool   `json:"enabled"`
	Rollout     int    `json:"rollout_pct"`
	Variants    []string `json:"variants,omitempty"`
	UpdatedAt   string `json:"updated_at"`
}

// ----- main --------------------------------------------------------------------

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)
	cfg := loadConfig()

	log.Printf("[START] target=%s target_gb=%d batch=%d prefix=%q",
		cfg.addr, cfg.targetGB, cfg.batchSize, cfg.keyPrefix)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	conn, reader, err := dial(cfg.addr, cfg.dialTimeout)
	if err != nil {
		log.Fatalf("[FATAL] dial %s: %v", cfg.addr, err)
	}
	defer conn.Close()

	id, _ := identify(conn, reader, cfg.cmdTimeout)
	log.Printf("[CONNECTED] run_id=%s", short(id))

	gen := newGenerator()
	targetBytes := int64(cfg.targetGB) * 1024 * 1024 * 1024
	var writtenBytes atomic.Int64
	var keyCount atomic.Int64
	var errCount atomic.Int64

	startTime := time.Now()
	batch := make([]string, 0, cfg.batchSize*2) // key, value alternating
	flush := func() bool {
		if len(batch) == 0 {
			return true
		}
		deadline := time.Now().Add(30 * time.Second)
		if err := conn.SetWriteDeadline(deadline); err != nil {
			log.Printf("[FLUSH] set write deadline failed: %v", err)
			return false
		}
		for i := 0; i < len(batch); i += 2 {
			if err := writeCmd(conn, "SET", batch[i], batch[i+1]); err != nil {
				errCount.Add(1)
				log.Printf("[FLUSH] write error at key %d/%d: %v", i/2, len(batch)/2, err)
				return false
			}
		}
		if err := conn.SetReadDeadline(deadline); err != nil {
			log.Printf("[FLUSH] set read deadline failed: %v", err)
			return false
		}
		for i := 0; i < len(batch); i += 2 {
			reply, err := readReply(reader)
			if err != nil {
				errCount.Add(1)
				log.Printf("[FLUSH] read error at reply %d/%d: %v", i/2, len(batch)/2, err)
				return false
			}
			if s, ok := reply.(string); !ok || s != "OK" {
				errCount.Add(1)
				continue
			}
			writtenBytes.Add(int64(len(batch[i]) + len(batch[i+1]) + 40))
			keyCount.Add(1)
		}
		batch = batch[:0]
		return true
	}

	// Periodically check the pipeline and flush.
	idx := 0
	logEvery := 50000
	nextLog := int64(logEvery)

loop:
	for {
		select {
		case <-stop:
			log.Println("[STOP] signal received — flushing final batch")
			break loop
		default:
		}

		key, val := gen.genKeyValue(cfg.keyPrefix, idx)
		batch = append(batch, key, val)
		idx++

		if len(batch) >= cfg.batchSize*2 || writtenBytes.Load() >= targetBytes {
			if !flush() {
				// Connection broken — reconnect and retry the batch.
				log.Printf("[RECONNECT] connection lost after %d keys, reconnecting...", keyCount.Load())
				conn.Close()
				var err error
				conn, reader, err = dial(cfg.addr, cfg.dialTimeout)
				if err != nil {
					log.Printf("[RECONNECT] dial failed: %v — retrying in 2s", err)
					time.Sleep(2 * time.Second)
					continue
				}
				id, _ := identify(conn, reader, cfg.cmdTimeout)
				log.Printf("[RECONNECTED] run_id=%s", short(id))
				continue
			}
			if writtenBytes.Load() >= targetBytes {
				break
			}
		}

		if keyCount.Load() >= nextLog {
			mb := float64(writtenBytes.Load()) / (1024 * 1024)
			elapsed := time.Since(startTime)
			rate := float64(writtenBytes.Load()) / elapsed.Seconds() / (1024 * 1024)
			log.Printf("[PROGRESS] keys=%d size=%.0f MB elapsed=%s rate=%.1f MB/s err=%d",
				keyCount.Load(), mb, elapsed.Round(time.Second), rate, errCount.Load())
			nextLog = keyCount.Load() + int64(logEvery)
		}
	}

	elapsed := time.Since(startTime)
	mb := float64(writtenBytes.Load()) / (1024 * 1024)
	log.Printf("[DONE] keys=%d size=%.0f MB (%.2f GB) elapsed=%s rate=%.1f MB/s err=%d",
		keyCount.Load(), mb, mb/1024, elapsed.Round(time.Second),
		float64(writtenBytes.Load())/elapsed.Seconds()/(1024*1024), errCount.Load())
}

// ----- data generation ---------------------------------------------------------

func (g *generator) genKeyValue(prefix string, idx int) (string, string) {
	switch idx % 8 {
	case 0:
		return g.genSession(prefix, idx)
	case 1:
		return g.genUserProfile(prefix, idx)
	case 2:
		return g.genProduct(prefix, idx)
	case 3:
		return g.genCategory(prefix, idx)
	case 4:
		return g.genAPIResponse(prefix, idx)
	case 5:
		return g.genRateLimit(prefix, idx)
	case 6:
		return g.genFeatureFlag(prefix, idx)
	default:
		return g.genStickyLB(prefix, idx)
	}
}

func (g *generator) genSession(prefix string, idx int) (string, string) {
	id := uuid4()
	now := time.Now().UTC()
	s := sessionData{
		UserID:    fmt.Sprintf("usr_%d", 100000+rndN(900000)),
		CreatedAt: now.Add(-time.Duration(1+rndN(7200)) * time.Minute).Format(time.RFC3339),
		ExpiresAt: now.Add(time.Duration(15+rndN(45)) * time.Minute).Format(time.RFC3339),
		UserAgent: g.userAgents[rndN(len(g.userAgents))],
		IP:        g.ips[rndN(len(g.ips))],
		Country:   g.countries[rndN(len(g.countries))],
		DeviceID:  uuid4()[:13],
	}
	b, _ := json.Marshal(s)
	return fmt.Sprintf("%s:session:%s", prefix, id), string(b)
}

func (g *generator) genUserProfile(prefix string, idx int) (string, string) {
	uid := fmt.Sprintf("usr_%d", 100000+rndN(900000))
	ts := time.Now().UTC().Add(-time.Duration(rndN(365*24)) * time.Hour).Format(time.RFC3339)
	p := userProfile{
		UserID:      uid,
		FirstName:   g.firstNames[rndN(len(g.firstNames))],
		LastName:    g.lastNames[rndN(len(g.lastNames))],
		Email:       fmt.Sprintf("%s.%s@%s", strings.ToLower(g.firstNames[rndN(len(g.firstNames))]), strings.ToLower(g.lastNames[rndN(len(g.lastNames))]), g.domains[rndN(len(g.domains))]),
		Country:     g.countries[rndN(len(g.countries))],
		CreatedAt:   ts,
		LastLoginAt: time.Now().UTC().Add(-time.Duration(rndN(48))*time.Hour).Format(time.RFC3339),
		Tier:        []string{"free", "pro", "enterprise"}[rndN(3)],
		Preferences: map[string]bool{
			"email_newsletter":   rndN(2) == 1,
			"push_notifications": rndN(2) == 1,
			"dark_mode":          rndN(2) == 1,
			"two_factor":         rndN(2) == 1,
		},
		Addresses: []userAddress{
			{
				Type:    "shipping",
				Street:  fmt.Sprintf("%d %s Street", 100+rndN(9900), []string{"Main", "Oak", "Elm", "Pine", "Maple", "Cedar", "Birch", "Walnut"}[rndN(8)]),
				City:    []string{"New York", "Berlin", "London", "Tokyo", "Paris", "Sydney", "Toronto", "Amsterdam", "Stockholm", "Singapore"}[rndN(10)],
				Zip:     fmt.Sprintf("%05d", rndN(100000)),
				Country: g.countries[rndN(len(g.countries))],
			},
		},
	}
	b, _ := json.Marshal(p)
	return fmt.Sprintf("%s:user:%s:profile", prefix, uid), string(b)
}

func (g *generator) genProduct(prefix string, idx int) (string, string) {
	tpl := g.products[rndN(len(g.products))]
	pid := fmt.Sprintf("prod_%s_%d", strings.ToLower(strings.ReplaceAll(tpl.Brand, " ", "")), 1000+rndN(9000))
	p := productDetail{
		ID:          pid,
		Name:        tpl.Name,
		Brand:       tpl.Brand,
		Category:    tpl.Category,
		Price:       float64(5+rndN(99500))/100 + 4.99,
		Currency:    "USD",
		InStock:     rndN(10) > 1,
		Rating:      float64(30+rndN(21))/10,
		ReviewCount: rndN(5000),
		Variants: []productVariant{
			{SKU: pid + "-S-BLK", Size: "S", Color: "Black", Price: float64(5+rndN(99500))/100 + 4.99, Stock: rndN(500)},
			{SKU: pid + "-M-BLK", Size: "M", Color: "Black", Price: float64(5+rndN(99500))/100 + 4.99, Stock: rndN(500)},
			{SKU: pid + "-L-BLK", Size: "L", Color: "Black", Price: float64(5+rndN(99500))/100 + 4.99, Stock: rndN(500)},
		},
		Attributes: map[string]string{
			"weight":     fmt.Sprintf("%dg", 100+rndN(5000)),
			"dimensions": fmt.Sprintf("%dx%dx%dmm", 50+rndN(300), 50+rndN(300), 10+rndN(100)),
			"material":   []string{"Aluminium", "Stainless Steel", "Carbon Fibre", "ABS Plastic", "Silicone", "Leather", "Cotton", "Polyester"}[rndN(8)],
		},
		ImageURLs: []string{
			fmt.Sprintf("https://cdn.example.com/products/%s/main.jpg", pid),
			fmt.Sprintf("https://cdn.example.com/products/%s/alt1.jpg", pid),
			fmt.Sprintf("https://cdn.example.com/products/%s/alt2.jpg", pid),
		},
	}
	b, _ := json.Marshal(p)
	return fmt.Sprintf("%s:cache:product:%s", prefix, pid), string(b)
}

func (g *generator) genCategory(prefix string, idx int) (string, string) {
	cat := g.categories[rndN(len(g.categories))]
	productIDs := make([]string, 50+rndN(200))
	for i := range productIDs {
		productIDs[i] = fmt.Sprintf("prod_%s_%d", strings.ToLower(cat.Slug), 1000+rndN(9000))
	}
	facets := []categoryFacet{
		{Key: "brand", Values: []categoryFacetVal{{Value: "SoundMax", Count: 42}, {Value: "VisioTech", Count: 38}, {Value: "KeyCraft", Count: 25}}},
		{Key: "price", Values: []categoryFacetVal{{Value: "0-50", Count: 120}, {Value: "50-200", Count: 89}, {Value: "200+", Count: 31}}},
	}
	cp := categoryPage{
		Slug:        cat.Slug,
		Name:        cat.Name,
		Description: cat.Description,
		ProductIDs:  productIDs,
		TotalCount:  500 + rndN(4500),
		Page:        1 + rndN(10),
		PageSize:    50,
		Facets:      facets,
		SortOption:  []string{"relevance", "price_asc", "price_desc", "rating", "newest"}[rndN(5)],
	}
	b, _ := json.Marshal(cp)
	return fmt.Sprintf("%s:cache:category:%s:page:%d", prefix, cat.Slug, 1+rndN(10)), string(b)
}

func (g *generator) genAPIResponse(prefix string, idx int) (string, string) {
	pathFmt := g.apiPaths[rndN(len(g.apiPaths))]
	path := fmt.Sprintf(pathFmt, fmt.Sprintf("usr_%d", 100000+rndN(900000)), fmt.Sprintf("prod_%d", 1000+rndN(9000)))
	resp := apiResponse{
		Data: []map[string]interface{}{
			{"id": fmt.Sprintf("item_%d", rndN(100000)), "name": "Sample Item", "score": float64(rndN(100)) / 100},
			{"id": fmt.Sprintf("item_%d", rndN(100000)), "name": "Sample Item", "score": float64(rndN(100)) / 100},
			{"id": fmt.Sprintf("item_%d", rndN(100000)), "name": "Sample Item", "score": float64(rndN(100)) / 100},
			{"id": fmt.Sprintf("item_%d", rndN(100000)), "name": "Sample Item", "score": float64(rndN(100)) / 100},
		},
		Meta: apiMeta{
			Page: 1, PageSize: 50, TotalCount: 200 + rndN(9800), TotalPages: 5 + rndN(20),
			NextCursor: uuid4(),
		},
		RequestID:  uuid4(),
		ServerTime: time.Now().UTC().Format(time.RFC3339),
	}
	hash := fmt.Sprintf("%x", pathHash(path))
	b, _ := json.Marshal(resp)
	return fmt.Sprintf("%s:cache:api:%s", prefix, hash), string(b)
}

func (g *generator) genRateLimit(prefix string, idx int) (string, string) {
	ip := g.ips[rndN(len(g.ips))]
	window := []string{"1m", "5m", "15m", "1h"}[rndN(4)]
	limit := 100 + rndN(900)
	return fmt.Sprintf("%s:ratelimit:%s:%s", prefix, ip, window), strconv.Itoa(limit)
}

func (g *generator) genFeatureFlag(prefix string, idx int) (string, string) {
	ft := g.featureFlags[rndN(len(g.featureFlags))]
	ff := featureFlag{
		Name:        ft.Name,
		Description: ft.Description,
		Enabled:     rndN(2) == 1,
		Rollout:     []int{0, 5, 10, 25, 50, 75, 100}[rndN(7)],
		Variants:    []string{"control", "treatment_a", "treatment_b"},
		UpdatedAt:   time.Now().UTC().Add(-time.Duration(rndN(720))*time.Hour).Format(time.RFC3339),
	}
	b, _ := json.Marshal(ff)
	return fmt.Sprintf("%s:feature:%s", prefix, ft.Name), string(b)
}

func (g *generator) genStickyLB(prefix string, idx int) (string, string) {
	sid := uuid4()
	backend := fmt.Sprintf("backend-%d.example.internal", 1+rndN(10))
	return fmt.Sprintf("%s:lb:sticky:%s", prefix, sid), backend
}

// ----- minimal RESP client (stdlib only) ---------------------------------------

type redisError struct{ msg string }

func (e *redisError) Error() string { return e.msg }

func dial(addr string, timeout time.Duration) (net.Conn, *bufio.Reader, error) {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, nil, err
	}
	return c, bufio.NewReader(c), nil
}

func identify(conn net.Conn, r *bufio.Reader, timeout time.Duration) (runID, role string) {
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return "", ""
	}
	if err := writeCmd(conn, "INFO"); err != nil {
		return "", ""
	}
	reply, err := readReply(r)
	if err != nil {
		return "", ""
	}
	s, _ := reply.(string)
	return infoField(s, "run_id:"), infoField(s, "role:")
}

func infoField(info, prefix string) string {
	for _, ln := range strings.Split(info, "\n") {
		ln = strings.TrimSpace(ln)
		if strings.HasPrefix(ln, prefix) {
			return strings.TrimSpace(ln[len(prefix):])
		}
	}
	return ""
}

func writeCmd(w io.Writer, args ...string) error {
	var b []byte
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(len(args)), 10)
	b = append(b, '\r', '\n')
	for _, a := range args {
		b = append(b, '$')
		b = strconv.AppendInt(b, int64(len(a)), 10)
		b = append(b, '\r', '\n')
		b = append(b, a...)
		b = append(b, '\r', '\n')
	}
	_, err := w.Write(b)
	return err
}

func readReply(r *bufio.Reader) (interface{}, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, errors.New("empty reply")
	}
	switch line[0] {
	case '+':
		return string(line[1:]), nil
	case '-':
		return nil, &redisError{msg: string(line[1:])}
	case ':':
		return strconv.ParseInt(string(line[1:]), 10, 64)
	case '$':
		n, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, fmt.Errorf("bad bulk length %q: %w", line[1:], err)
		}
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(string(line[1:]))
		if err != nil {
			return nil, fmt.Errorf("bad array length %q: %w", line[1:], err)
		}
		if n < 0 {
			return nil, nil
		}
		arr := make([]interface{}, n)
		for i := 0; i < n; i++ {
			el, err := readReply(r)
			if err != nil {
				return nil, err
			}
			arr[i] = el
		}
		return arr, nil
	}
	return nil, fmt.Errorf("unknown reply type %q", line[0])
}

func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	n := len(line)
	if n >= 2 && line[n-2] == '\r' {
		return line[:n-2], nil
	}
	return line[:n-1], nil
}

// ----- helpers -----------------------------------------------------------------

func mustRand() *randReader { return &randReader{} }

type randReader struct{}

func (r *randReader) Int(max int) int {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(max)))
	return int(n.Int64())
}

func rndN(n int) int { return rnd.Int(n) }

func uuid4() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func pathHash(s string) uint32 {
	var h uint32
	for i := 0; i < len(s); i++ {
		h = h*31 + uint32(s[i])
	}
	return h
}

func short(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	if id == "" {
		return "?"
	}
	return id
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		log.Printf("[WARN] invalid %s=%q, using %s", key, v, def)
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Printf("[WARN] invalid %s=%q, using %d", key, v, def)
	}
	return def
}
