package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	log "github.com/sirupsen/logrus"

	sq "github.com/Masterminds/squirrel"
	"github.com/google/uuid"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/types"
	_ "github.com/lib/pq"
	"github.com/tidwall/gjson"
)

var allClubs = map[string]Club{
	"01": Club{"01", "Auckland City"},
	"09": Club{"09", "Britomart"},
	"13": Club{"13", "Newmarket"},
	"06": Club{"06", "Takapuna"},
}
var analyticsDB *sqlx.DB
var classesDB *sqlx.DB

func init() {
	// Logging
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = time.RFC3339Nano
	customFormatter.FullTimestamp = true
	log.SetFormatter(customFormatter)
	log.SetOutput(os.Stdout)
	log.SetLevel(log.TraceLevel)

	// AnalyticsDB
	var err error

	passwd := os.Getenv("POSTGRES_PASSWORD")
	if passwd == "" {
		log.Fatalf("Failed to get postgres password")
	}
	connStr := fmt.Sprintf("user=postgres dbname=analytics sslmode=disable password=%s", passwd)
	analyticsDB, err = sqlx.Connect("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to open analytics db - %s\n", err)
	}

	connStr = fmt.Sprintf("user=postgres dbname=classes sslmode=disable password=%s", passwd)
	classesDB, err = sqlx.Connect("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to open classes db - %s\n", err)
	}
	var createEventTable string
	createEventTable = `
CREATE TABLE IF NOT EXISTS events (
  id VARCHAR(40) PRIMARY KEY,
  user_id VARCHAR(40),
  session_id VARCHAR(40),
  data JSONB,
  action VARCHAR(40),
  created_at TIMESTAMPTZ
);`

	_, err = analyticsDB.Exec(createEventTable)
	if err != nil {
		log.Fatalf("Failed to create event table - %s\n", err)
		return
	}
	var createClassesTable string

	createClassesTable = `
CREATE TABLE IF NOT EXISTS classes (
  id VARCHAR(40) PRIMARY KEY,
  name VARCHAR(40),
  code VARCHAR(40),
  description TEXT,
  club_id VARCHAR(40),
  duration INT,
  start_datetime TIMESTAMPTZ,
  end_datetime TIMESTAMPTZ,
  is_virtual_class BOOLEAN
)`

	_, err = classesDB.Exec(createClassesTable)
	if err != nil {
		log.Fatalf("Failed to create classes table - %s\n", err)
		return
	}

	createClassTypeTable := `
CREATE TABLE IF NOT EXISTS class_types(
   id VARCHAR(40),
   name VARCHAR(40)
)`
	_, err = classesDB.Exec(createClassTypeTable)
	if err != nil {
		log.Fatalf("Failed to create class type table - %s\n", err)
		return
	}

	err = deleteClasses(classesDB)
	if err != nil {
		log.Fatalf("Failed to truncate class table - %s\n", err)
		return
	}

	err = deleteClassTypes(classesDB)
	if err != nil {
		log.Fatalf("Failed to truncate class type table - %s\n", err)
		return
	}

}

type AnalyticsEvent struct {
	ID        string         `sql:"id"`
	User      string         `json:"user" sql:"user_id"`
	Session   string         `json:"session" sql:"session_id"`
	Data      types.JSONText `json:"data" sql:"data"`
	Action    string         `json:"action" sql:"action"`
	CreatedAt time.Time      `json:"-" sql:"created_at"`
}

type Club struct {
	ID   string `json:"ClubCode" storm:"index" sql:"club_id"`
	Name string `json:"Name"`
}

type Query struct {
	Name      []string    `json:"name"`
	Club      []Club      `json:"club"`
	Date      []time.Time `json:"date"`
	Hour      []int       `json:"hour"`
	IsVirtual bool        `json:"isVirtual"`
}

type Instructor struct {
	ID          string `json:"InstructorId"`
	Name        string `json:"Name"`
	MemberID    string `json:"InstructorMemberId"`
	Description string `json:"Description"`
}

type Class struct {
	ID                  string     `json:"ClassInstanceId" sql:"id"`
	Name                string     `json:"ClassName" sql:"name"`
	Code                string     `json:"ClassCode" sql:"code"`
	Club                Club       `json:"Club" sql:"club"`
	DefinitionID        string     `json:"ClassDefinitionId"`
	Description         string     `json:"ClassDescription" sql:"description"`
	Duration            int        `json:"Duration" sql:"duration"`
	StartDateTime       time.Time  `json:"StartDateTime" sql:"start_datetime"`
	EndDateTime         time.Time  `json:"EndDateTime" sql:"end_datetime"`
	Equipment           string     `json:"Equipment"`
	ExerciseType        string     `json:"ExerciseType"`
	Intensity           string     `json:"Intensity"`
	IsVirtualClass      bool       `json:"IsVirtualClass" sql:"is_virtual_class"`
	MainInstructor      Instructor `json:"MainInstructor"`
	SecondaryInstructor Instructor `json:"SecondaryInstructor"`
	Site                struct {
		Capacity int    `json:"Capacity"`
		ID       string `json:"SiteId"`
		Name     string `json:"SiteName"`
	} `json:"Site"`
	Status string `json:"Status"`
}

// Remove some of the unncessary fields with custom marshaller
func (c Class) MarshalJSON() ([]byte, error) {
	cs := map[string]interface{}{}
	cs["ID"] = c.ID
	cs["Code"] = c.Code
	cs["Description"] = c.Description
	cs["Name"] = c.Name
	cs["Club"] = c.Club.ID
	cs["Duration"] = c.Duration
	cs["StartDatetime"] = c.StartDateTime
	cs["EndDatetime"] = c.EndDateTime
	cs["IsVirtualClass"] = c.IsVirtualClass
	return json.Marshal(cs)
}

type ClassType struct {
	ID   string `storm:"id" json:"Key" sql:"id"`
	Name string `json:"Value" sql:"name"`
}

func ClassesHandler(w http.ResponseWriter, r *http.Request) {
	// Get query parameters
	params := r.URL.Query()
	var q = Query{}

	// Ensure we're returning JSON
	w.Header().Set("Content-Type", "application/json")

	// Parse names
	n := params.Get("name")
	if n != "" {
		ns := strings.Split(string(n), ",")
		q.Name = ns
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
		q.Date = dates
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
		q.Club = clubs
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
		q.Hour = hours
	}

	// Parse Virtual Class
	v := params.Get("virtual")
	if v != "" {
		virt, err := strconv.ParseBool(v)
		if err != nil {
			log.Errorf("Failed to parse virtual string- %s \n", err)
			// TODO return 400
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("Failed to parse virtual parameter"))
			return
		}
		q.IsVirtual = virt
	}

	classes, err := queryClasses(classesDB, q)
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
	classTypes, err := queryClassTypes(classesDB)
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
	_, err = analyticsDB.NamedExec(`INSERT INTO events VALUES (:id, :user, :session, :data, :action, :createdat)`, event)
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
	// Check if the DB has data
	var c []Class
	err := classesDB.Select(&c, "SELECT * FROM classes;")
	if err != nil {
		log.Errorf("Failed to return classes from healthcheck")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to get classes"))
		return
	}
	if len(c) == 0 {
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

func deleteClasses(db *sqlx.DB) error {
	var truncateClassTable string
	truncateClassTable = `TRUNCATE classes`

	_, err := classesDB.Exec(truncateClassTable)
	if err != nil {
		return err
	}
	return nil
}

func deleteClassTypes(db *sqlx.DB) error {
	var truncateClassTypeTable string
	truncateClassTypeTable = `TRUNCATE class_types`
	_, err := classesDB.Exec(truncateClassTypeTable)
	if err != nil {
		return err
	}
	return nil
}

func saveClasses(db *sqlx.DB, classes []Class) error {
	log.Infof("Saving %d classes\n", len(classes))
	t1 := time.Now()
	for _, c := range classes {
		insertStmt := `
INSERT INTO classes(
    id,
    name,
    code,
    description,
    club_id,
    duration,
    start_datetime,
    end_datetime,
    is_virtual_class)
  VALUES (?,?,?,?,?,?,?,?,?)
`
		insertStmt = db.Rebind(insertStmt)
		_, err := db.Exec(
			insertStmt,
			c.ID,
			c.Name,
			c.Code,
			c.Description,
			c.Club.ID,
			c.Duration,
			c.StartDateTime,
			c.EndDateTime,
			c.IsVirtualClass,
		)
		if err != nil {
			log.Errorf("Failed to save class - %s\n", err)
			return err
		}
	}
	t2 := time.Now()
	diff := t2.Sub(t1)
	log.Infof("Classes saved in %s\n", diff)
	return nil
}

func saveClassTypes(db *sqlx.DB, classTypes []ClassType) error {
	log.Infof("Saving %d classtypes\n", len(classTypes))
	t1 := time.Now()
	for _, c := range classTypes {
		insertStmt := `INSERT INTO class_types(id, name) VALUES (?,?)`
		insertStmt = db.Rebind(insertStmt)
		_, err := db.Exec(insertStmt, c.ID, c.Name)
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

func queryClasses(db *sqlx.DB, query Query) ([]Class, error) {
	classes := make([]Class, 0)
	t1 := time.Now()

	s := sq.Select("*").From("classes")

	if len(query.Name) > 0 {
		s = s.Where(sq.Eq{"code": query.Name})
	}

	if len(query.Club) > 0 {
		var clubQueries []string
		for _, c := range query.Club {
			clubQueries = append(clubQueries, c.ID)
		}

		s = s.Where(sq.Eq{"club_id": clubQueries})
	}

	if len(query.Date) > 0 {
		var dateQueries []string
		for _, d := range query.Date {
			dateQueries = append(dateQueries, d.Format("2006-01-02"))
		}
		// Here I assume the user is in NZ!
		s = s.Where(sq.Eq{"DATE(start_datetime + INTERVAL '12 hour')": dateQueries})

	}

	if len(query.Hour) > 0 {
		var hourQueries []int
		for _, h := range query.Hour {
			hourQueries = append(hourQueries, (h+12)%24)
		}

		s = s.Where(sq.Eq{"EXTRACT(HOUR from start_datetime)": hourQueries})

	}

	// If it's false, we should hide virtual classes
	if query.IsVirtual == false {
		s = s.Where(sq.Eq{"is_virtual_class": false})
	}

	// Order by next class and classes after now
	s = s.Where(sq.Gt{"start_datetime": time.Now()})
	s = s.OrderBy("start_datetime ASC")
	selectStmt, args, err := s.ToSql()
	if err != nil {
		return nil, err

	}

	selectStmt = db.Rebind(selectStmt)
	rows, err := db.Queryx(selectStmt, args...)
	if err != nil {
		return nil, err
	}

	log.Tracef("Finished getting classes from DB in %s \n", time.Now().Sub(t1))
	for rows.Next() {
		var c Class
		err = rows.Scan(
			&c.ID,
			&c.Name,
			&c.Code,
			&c.Description,
			&c.Club.ID,
			&c.Duration,
			&c.StartDateTime,
			&c.EndDateTime,
			&c.IsVirtualClass,
		)
		if err != nil {
			return nil, err
		}
		classes = append(classes, c)

	}

	log.Infof("Returning %d classes\n", len(classes))
	return classes, nil
}

func queryClassTypes(db *sqlx.DB) ([]ClassType, error) {
	var allClassTypes []ClassType
	err := db.Select(&allClassTypes, "SELECT * FROM class_types;")
	if err != nil {
		return nil, err
	}
	log.Infof("Returning all class types - %d\n", len(allClassTypes))
	return allClassTypes, nil

}

func main() {

	// Create the DB
	defer classesDB.Close()
	defer analyticsDB.Close()

	classes, classTypes, err := getClasses()
	if err != nil {
		log.Errorf("Failed to get classes and classTypes with error - %s\n", err)
	}

	err = saveClasses(classesDB, classes)
	if err != nil {
		log.Errorf("Failed to save classes with error - %s\n", err)
	}

	err = saveClassTypes(classesDB, classTypes)
	if err != nil {
		log.Errorf("Failed to save class types with error - %s\n", err)
	}

	// Periodically get new classes
	ticker := time.NewTicker(6 * time.Hour)
	go func() {
		for {
			select {
			case <-ticker.C:
				log.Info("Refreshing classes")
				err = deleteClasses(classesDB)
				if err != nil {
					log.Errorf("Failed to delete classes with error - %s\n", err)

				}

				classes, classTypes, err := getClasses()
				if err != nil {
					log.Errorf("Failed to get classes and classTypes with error - %s\n", err)
				}

				err = saveClasses(classesDB, classes)
				if err != nil {
					log.Errorf("Failed to save classes with error - %s\n", err)

				}
				err = deleteClassTypes(classesDB)
				if err != nil {
					log.Errorf("Failed to delete class types with error - %s\n", err)

				}
				err = saveClassTypes(classesDB, classTypes)
				if err != nil {
					log.Errorf("Failed to save class types with error - %s\n", err)

				}
			}
		}
	}()

	r := mux.NewRouter()

	r.HandleFunc("/classes/", ClassesHandler).Methods("GET")
	r.HandleFunc("/classtypes/", ClassTypesHandler).Methods("GET")
	r.HandleFunc("/analytics/", AnalyticsHandler).Methods("POST")
	r.HandleFunc("/healthcheck/", HealthCheckHandler).Methods("GET")

	fs := http.FileServer(http.Dir("./build"))
	r.PathPrefix("/").Handler(http.StripPrefix("/", fs))

	// Add CORS and compression handlers
	h := handlers.CompressHandler(handlers.CORS()(r))

	srv := &http.Server{
		Handler:      h,
		Addr:         ":9000",
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
	}
	log.Fatal(srv.ListenAndServe())
}
