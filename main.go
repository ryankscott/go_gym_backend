package main

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/asdine/storm"
	"github.com/asdine/storm/q"
	"github.com/google/uuid"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/tidwall/gjson"
)

var allClubs = map[string]Club{
	"01": Club{"01", "Auckland City"},
	"09": Club{"09", "Britomart"},
	"13": Club{"13", "Newmarket"},
	"06": Club{"06", "Takapuna"},
}
var db *storm.DB

func init() {
	// Logging
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = time.RFC3339Nano
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)
	log.SetOutput(os.Stdout)
	log.SetLevel(log.TraceLevel)

	// AnalyticsDB
	// var err error
	// analyticsDB, err = sql.Open("sqlite3", "./analytics.db")
	// if err != nil {
	// 	log.Fatalf("Failed to open analytics db - %s\n", err)
	// }
	// defer analyticsDB.Close()
	// sqlStmt := `
	// CREATE TABLE event (id integer not null primary key, user text, session text, data text, action text, created_at datetime);
	// `
	// _, err = analyticsDB.Exec(sqlStmt)
	// if err != nil {
	// 	log.Fatalf("Failed to open analytics db - %s\n", err)
	// 	return
	// }

}

type AnalyticsEvent struct {
	ID        string      `sql:"id"`
	User      string      `json:"user" sql:"user"`
	Session   string      `json:"session" sql:"session"`
	Data      interface{} `json:"data" sql:"data"`
	Action    string      `json:"action" sql:"action"`
	CreatedAt time.Time   `json:"-" sql:"created_at"`
}

type Club struct {
	ClubCode string `json:"ClubCode" storm:"index"`
	Name     string `json:"Name"`
}

type Query struct {
	name []string    `json:"name"`
	club []Club      `json:"club"`
	date []time.Time `json:"date"`
	hour []int       `json:"hour"`
}

type Instructor struct {
	Description        string `json:"Description"`
	InstructorID       string `json:"InstructorId"`
	InstructorMemberID string `json:"InstructorMemberId"`
	Name               string `json:"Name"`
}

type Class struct {
	ClassCode           string     `json:"ClassCode"`
	ClassDefinitionID   string     `json:"ClassDefinitionId"`
	ClassDescription    string     `json:"ClassDescription"`
	ClassInstanceID     string     `storm:"id" json:"ClassInstanceId"`
	ClassName           string     `storm:"index" json:"ClassName"`
	Club                Club       `storm:"index" json:"Club"`
	Duration            int        `storm:"index" json:"Duration"`
	StartDay            int        `storm:"index" json:-`
	StartHour           int        `storm:"index" json:-`
	StartDateTime       time.Time  `storm:"index" json:"StartDateTime"`
	EndDateTime         time.Time  `json:"EndDateTime"`
	Equipment           string     `json:"Equipment"`
	ExerciseType        string     `json:"ExerciseType"`
	Intensity           string     `json:"Intensity"`
	IsVirtualClass      bool       `json:"IsVirtualClass"`
	MainInstructor      Instructor `json:"MainInstructor"`
	SecondaryInstructor Instructor `json:"SecondaryInstructor"`
	Site                struct {
		Capacity int    `json:"Capacity"`
		SiteID   string `json:"SiteId"`
		SiteName string `json:"SiteName"`
	} `json:"Site"`
	Status string `json:"Status"`
}
type ClassAlias Class

func (c Class) MarshalJSON() ([]byte, error) {
	return json.Marshal(NewJSONClass(c))
}
func (c *Class) UnmarshalJSON(data []byte) error {
	var cd JSONClass
	if err := json.Unmarshal(data, &cd); err != nil {
		return err
	}
	*c = cd.Class()
	return nil
}

func NewJSONClass(class Class) JSONClass {
	return JSONClass{
		ClassAlias(class),
		class.StartDateTime.Day(),
		class.StartDateTime.Hour(),
	}
}

type JSONClass struct {
	ClassAlias
	StartDay  int `json:"StartDay"`
	StartHour int `json:"StartHour"`
}

func (cd JSONClass) Class() Class {
	class := Class(cd.ClassAlias)
	class.StartDay = cd.StartDay
	class.StartHour = cd.StartHour
	return class
}

type ClassType struct {
	Key   string `storm:"id" json:"Key"`
	Value string `json:"Value"`
}

func ClassesHandler(w http.ResponseWriter, r *http.Request) {
	/* name="BodyPump, RPM"&club="Auckland City"&date="2018-07-18,2018-07-19"&hour=11 */
	// Get query parameters
	params := r.URL.Query()
	var q = Query{}

	// Ensure we're returning JSON
	w.Header().Set("Content-Type", "application/json")

	// Parse names
	n := params.Get("name")
	if n != "" {
		ns := strings.Split(string(n), ",")
		q.name = ns
	}

	// Parse dates
	d := params.Get("date")
	if d != "" {
		ds := strings.Split(string(d), ",")
		var dates []time.Time
		for _, v := range ds {
			t, err := time.Parse("2006-01-02", v)
			if err != nil {
				log.Errorf("Failed to parse date string - %s \n", err)
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("Failed to parse date parameter"))
				return
			}
			dates = append(dates, t)
		}
		q.date = dates
	}

	// Parse clubs
	c := params.Get("club")
	if c != "" {
		cs := strings.Split(string(c), ",")
		var clubs []Club
		for _, v := range cs {
			var club Club
			club = allClubs[v]
			clubs = append(clubs, club)
		}
		q.club = clubs
	}

	// Parse hours
	h := params.Get("hour")
	if h != "" {
		hs := strings.Split(string(h), ",")
		var hours []int
		for _, v := range hs {
			hrs, err := strconv.Atoi(v)
			if err != nil {
				log.Errorf("Failed to parse hours string - %s \n", err)
				// TODO return 400
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("Failed to parse hours parameter"))
				return
			}
			hours = append(hours, hrs)

		}
		q.hour = hours
	}
	classes, err := queryClasses(db, q)
	if err != nil {
		log.Errorf("Failed to query classes - %s \n", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to return classes"))
		return
	}

	t1 := time.Now()
	jClasses, err := json.Marshal(classes)
	if err != nil {
		log.Errorf("Failed to marshal classes to JSON - %s \n", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to return classes"))
		return
	}
	log.Tracef("Finished marshalling classes in %s", time.Now().Sub(t1))

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(jClasses))
}

func ClassTypesHandler(w http.ResponseWriter, r *http.Request) {
	// Ensure we're returning JSON
	w.Header().Set("Content-Type", "application/json")
	classTypes, err := queryClassTypes(db)
	if err != nil {
		log.Errorf("Failed to get classTypes - %s \n", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to return classTypes"))
		return
	}
	jClassTypes, err := json.Marshal(classTypes)
	if err != nil {
		log.Errorf("Failed to marshal classTypes to JSON - %s \n", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to return classTypes"))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(jClassTypes))
}

func AnalyticsHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorf("Error reading body - %s \n", err)
	}
	var event AnalyticsEvent
	err = json.Unmarshal(body, &event)
	if err != nil {
		log.Errorf("Failed to unmarshal JSON to analytics event - %s \n", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to save analytics event"))
		return
	}
	// Create a UUID and timestamp
	event.ID = uuid.New().String()
	event.CreatedAt = time.Now()

	// Save it
	err = db.Save(&event)
	if err != nil {
		log.Errorf("Failed to save analytics event - %s \n", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to save analytics event"))
		log.Infof("Failed to save analytics event - %s\n", err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	// Check if the UI is up
	_, err := http.Get("http://localhost:3000/")
	if err != nil {
		log.Errorf("Failed to check if UI is up from healtcheck")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to get UI"))
		return
	}

	// Check if the DB has data
	var c []Class
	err = db.All(&c)
	if err != nil {
		log.Errorf("Failed to return classes from healthcheck")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to get classes"))
		return
	}

	w.WriteHeader(http.StatusOK)
	return
}

func getClasses() ([]Class, []ClassType, error) {

	log.Infof("Fetching classes\n")
	resp, err := http.Post("https://www.lesmills.co.nz/api/timetable/get-timetable-epi", "application/x-www-form-urlencoded", strings.NewReader("Club=01,09,13,06"))

	if err != nil {
		log.Errorf("Failed to retrieve classes from Les Mills - %s \n", err)
		return nil, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Errorf("Returned a non-OK response code (%d) from Les Mills\n", resp.StatusCode)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("Failed to read response body from Les Mills - %s \n", err)
	}
	jsonResponse := (string(body))
	cs := gjson.Get(jsonResponse, "Classes")
	cts := gjson.Get(jsonResponse, "ClassType")

	// Get Classes
	var classes []Class
	err = json.Unmarshal([]byte(cs.String()), &classes)
	if err != nil {
		log.Errorf("Failed to unmarshal classes %s\n", err)
		return nil, nil, err
	}
	log.Infof("Fetched %d classes\n", len(classes))

	// Get ClassTypes
	var classTypes []ClassType
	err = json.Unmarshal([]byte(cts.String()), &classTypes)
	if err != nil {
		log.Errorf("Failed to unmarshal classtypes %s\n", err)
		return nil, nil, err
	}
	log.Infof("Fetched %d class types\n", len(classTypes))
	return classes, classTypes, nil
}

func saveClasses(db *storm.DB, classes []Class) error {
	log.Infof("Saving %d classes\n", len(classes))
	t1 := time.Now()
	for _, c := range classes {
		err := db.Save(&c)
		if err != nil {
			log.Errorf("Failed to save classes - %s\n", err)
			return err
		}
	}
	t2 := time.Now()
	diff := t2.Sub(t1)
	log.Infof("Classes saved in %s\n", diff)
	return nil
}

func saveClassTypes(db *storm.DB, classTypes []ClassType) error {
	log.Infof("Saving %d classtypes\n", len(classTypes))
	t1 := time.Now()
	for _, c := range classTypes {
		err := db.Save(&c)
		if err != nil {
			log.Errorf("Failed to save class types - %s\n", err)
			return err
		}
	}
	t2 := time.Now()
	diff := t2.Sub(t1)
	log.Infof("Classtypes saved in %s\n", diff)
	return nil

}

func createMatcher(m ...[]q.Matcher) q.Matcher {
	var queries []q.Matcher
	for _, v := range m {
		// If there's contents of the matcher then save it
		if len(v) > 0 {
			queries = append(queries, q.Or(v...))
		}
	}
	if len(queries) == 0 {
		return nil
	}
	allQueries := q.And(queries...)
	return allQueries
}

func queryClasses(db *storm.DB, query Query) ([]Class, error) {
	var classes []Class
	// Extract the names
	var nameQueries []q.Matcher
	log.Infof("Querying for classes using %s as name parameters\n", query.name)
	for _, v := range query.name {
		nameQueries = append(nameQueries, q.Eq("ClassCode", v))
	}

	// Extract the clubs
	var clubQueries []q.Matcher
	log.Infof("Querying for classes using %s as club parameters\n", query.club)
	for _, v := range query.club {
		clubQueries = append(clubQueries, q.Eq("Club", v))
	}

	// Extract the dates
	var dateQueries []q.Matcher
	log.Infof("Querying for classes using %s as date parameters\n", query.date)
	location, err := time.LoadLocation("Pacific/Auckland")
	if err != nil {
		log.Errorf("Failed to load location - %s\n", err)
		return nil, err
	}
	for _, v := range query.date {
		endOfDay := time.Date(v.Year(), v.Month(), v.Day(), 23, 59, 59, 0, location)
		startOfDay := time.Date(v.Year(), v.Month(), v.Day(), 0, 0, 0, 0, location)
		dateQueries = append(dateQueries, q.And(q.Gt("StartDateTime", startOfDay), q.Lt("StartDateTime", endOfDay)))
	}
	// Only return last queries after now
	if len(dateQueries) == 0 {
		dateQueries = append(dateQueries, q.Gt("StartDateTime", time.Now()))
	}

	// Extract the hours
	var hourQueries []q.Matcher
	log.Infof("Querying for hours using %d as hour parameters\n", query.hour)
	for _, v := range query.hour {
		hourQueries = append(hourQueries, q.Eq("StartHour", v))
	}

	// Combine the matchers
	matcher := createMatcher(nameQueries, clubQueries, dateQueries, hourQueries)
	// If we don't have query parameters

	t1 := time.Now()
	if matcher == nil {
		log.Errorf("We have no matchers so returning all classes\n")
		err = db.All(&classes)

	} else {
		err = db.Select(matcher).OrderBy("StartDateTime").Find(&classes)
	}
	if err != nil {
		if err == storm.ErrNotFound {
			log.Errorf("Returning no classes without error\n")
			return []Class{}, nil

		}
		log.Errorf("Failed to select classes - %s\n", err)
		return nil, err
	}
	log.Tracef("Finished getting classes from DB in %s \n", time.Now().Sub(t1))

	log.Infof("Returning %d classes\n", len(classes))
	return classes, nil
}

func queryClassTypes(db *storm.DB) ([]ClassType, error) {
	var allClassTypes []ClassType
	err := db.All(&allClassTypes)
	if err != nil {
		return nil, err
	}
	return allClassTypes, nil

}

func main() {

	// Create the DB
	var err error
	db, err = storm.Open("classes.db")
	if err != nil {
		log.Fatalf("Failed to open database with error  - %s \n", err)
	}

	// Drop all old data
	err = db.Drop("Class")
	log.Infof("Dropping all old class data")
	if err != nil {
		log.Errorf("Failed to drop class data - %s \n", err)
	}
	err = db.Drop("ClassType")
	log.Infof("Dropping all old classtypes data")
	if err != nil {
		log.Infof("Failed to drop classtypes data - %s \n", err)
	}

	classes, classTypes, err := getClasses()
	if err != nil {
		log.Errorf("Failed to get classes and classTypes with error - %s\n", err)
	}

	err = saveClasses(db, classes)
	if err != nil {
		log.Errorf("Failed to save classes with error - %s\n", err)
	}

	err = saveClassTypes(db, classTypes)
	if err != nil {
		log.Errorf("Failed to save class types with error - %s\n", err)
	}

	// Periodically get new classes
	ticker := time.NewTicker(6 * time.Hour)
	go func() {
		for {
			select {
			case <-ticker.C:
				err = saveClasses(db, classes)
				if err != nil {
					log.Errorf("Failed to save classes with error - %s\n", err)

				}

				err = saveClassTypes(db, classTypes)
				if err != nil {
					log.Errorf("Failed to save class types with error - %s\n", err)

				}
			}
		}
	}()

	defer db.Close()

	r := mux.NewRouter()

	r.HandleFunc("/classes/", ClassesHandler).Methods("GET")
	r.HandleFunc("/classtypes/", ClassTypesHandler).Methods("GET")
	r.HandleFunc("/analytics/", AnalyticsHandler).Methods("POST")
	r.HandleFunc("/healthcheck/", HealthCheckHandler).Methods("GET")

	fs := http.FileServer(http.Dir("./build"))
	r.PathPrefix("/").Handler(http.StripPrefix("/", fs))

	// Bind to a port and pass our router in
	srv := &http.Server{
		Handler:      handlers.CORS()(r),
		Addr:         ":9000",
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
	}
	log.Fatal(srv.ListenAndServe())
}
