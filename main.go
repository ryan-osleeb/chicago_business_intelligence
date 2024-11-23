package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/kelvins/geocoder"
	_ "github.com/lib/pq"
)

type TaxiTripsJsonRecords []struct {
	Trip_id                    string `json:"trip_id"`
	Trip_start_timestamp       string `json:"trip_start_timestamp"`
	Trip_end_timestamp         string `json:"trip_end_timestamp"`
	Pickup_centroid_latitude   string `json:"pickup_centroid_latitude"`
	Pickup_centroid_longitude  string `json:"pickup_centroid_longitude"`
	Dropoff_centroid_latitude  string `json:"dropoff_centroid_latitude"`
	Dropoff_centroid_longitude string `json:"dropoff_centroid_longitude"`
}

type UnemploymentJsonRecords []struct {
	Community_area                             string `json:"community_area"`
	Community_area_name                        string `json:"community_area_name"`
	Birth_rate                                 string `json:"birth_rate"`
	General_fertility_rate                     string `json:"general_fertility_rate"`
	Low_birth_weight                           string `json:"low_birth_weight"`
	Prenatal_care_beginning_in_first_trimester string `json:"prenatal_care_beginning_in_first_trimester"`
	Preterm_births                             string `json:"preterm_births"`
	Teen_birth_rate                            string `json:"teen_birth_rate"`
	Assault_homicide                           string `json:"assault_homicide"`
	Breast_cancer_in_females                   string `json:"breast_cancer_in_females"`
	Cancer_all_sites                           string `json:"cancer_all_sites"`
	Colorectal_cancer                          string `json:"colorectal_cancer"`
	Diabetes_related                           string `json:"diabetes_related"`
	Firearm_related                            string `json:"firearm_related"`
	Infant_mortality_rate                      string `json:"infant_mortality_rate"`
	Lung_cancer                                string `json:"lung_cancer"`
	Prostate_cancer_in_males                   string `json:"prostate_cancer_in_males"`
	Stroke_cerebrovascular_disease             string `json:"stroke_cerebrovascular_disease"`
	Childhood_blood_lead_level_screening       string `json:"childhood_blood_lead_level_screening"`
	Childhood_lead_poisoning                   string `json:"childhood_lead_poisoning"`
	Gonorrhea_in_females                       string `json:"gonorrhea_in_females"`
	Gonorrhea_in_males                         string `json:"gonorrhea_in_males"`
	Tuberculosis                               string `json:"tuberculosis"`
	Below_poverty_level                        string `json:"below_poverty_level"`
	Crowded_housing                            string `json:"crowded_housing"`
	Dependency                                 string `json:"dependency"`
	No_high_school_diploma                     string `json:"no_high_school_diploma"`
	Per_capita_income                          string `json:"per_capita_income"`
	Unemployment                               string `json:"unemployment"`
}

type BuildingPermitsJsonRecords []struct {
	Id                     string `json:"id"`
	Permit_Code            string `json:"permit_"`
	Permit_type            string `json:"permit_type"`
	Review_type            string `json:"review_type"`
	Application_start_date string `json:"application_start_date"`
	Issue_date             string `json:"issue_date"`
	Processing_time        string `json:"processing_time"`
	Street_number          string `json:"street_number"`
	Street_direction       string `json:"street_direction"`
	Street_name            string `json:"street_name"`
	Work_description       string `json:"work_description"`
	Building_fee_paid      string `json:"building_fee_paid"`
	Zoning_fee_paid        string `json:"zoning_fee_paid"`
	Other_fee_paid         string `json:"other_fee_paid"`
	Subtotal_paid          string `json:"subtotal_paid"`
	Building_fee_unpaid    string `json:"building_fee_unpaid"`
	Zoning_fee_unpaid      string `json:"zoning_fee_unpaid"`
	Other_fee_unpaid       string `json:"other_fee_unpaid"`
	Subtotal_unpaid        string `json:"subtotal_unpaid"`
	Building_fee_waived    string `json:"building_fee_waived"`
	Zoning_fee_waived      string `json:"zoning_fee_waived"`
	Other_fee_waived       string `json:"other_fee_waived"`
	Subtotal_waived        string `json:"subtotal_waived"`
	Total_fee              string `json:"total_fee"`
	Contact_1_type         string `json:"contact_1_type"`
	Contact_1_name         string `json:"contact_1_name"`
	Contact_1_city         string `json:"contact_1_city"`
	Contact_1_state        string `json:"contact_1_state"`
	Contact_1_zipcode      string `json:"contact_1_zipcode"`
	Reported_cost          string `json:"reported_cost"`
	Community_area         string `json:"community_area"`
	Census_tract           string `json:"census_tract"`
	Ward                   string `json:"ward"`
	Xcoordinate            string `json:"xcoordinate"`
	Ycoordinate            string `json:"ycoordinate"`
}

type ZipcodeCovidRecords struct {
	Zip_code                           string `json:"zip_code"`
	Week_number                        string `json:"week_number"`
	Week_start                         string `json:"week_start"`
	Week_end                           string `json:"week_end"`
	Cases_weekly                       string `json:"cases_weekly"`
	Cases_cumulative                   string `json:"cases_cumulative"`
	Case_rate_weekly                   string `json:"case_rate_weekly"`
	Case_rate_cumulative               string `json:"case_rate_cumulative"`
	Tests_weekly                       string `json:"tests_weekly"`
	Tests_cumulative                   string `json:"tests_cumulative"`
	Test_rate_weekly                   string `json:"test_rate_weekly"`
	Test_rate_cumulative               string `json:"test_rate_cumulative"`
	Percent_tested_positive_weekly     string `json:"percent_tested_positive_weekly"`
	Percent_tested_positive_cumulative string `json:"percent_tested_positive_cumulative"`
	Deaths_weekly                      string `json:"deaths_weekly"`
	Deaths_cumulative                  string `json:"deaths_cumulative"`
	Death_rate_weekly                  string `json:"death_rate_weekly"`
	Death_rate_cumulative              string `json:"death_rate_cumulative"`
}

type CCVIRecords struct {
	GeographyType                    string  `json:"geography_type"`
	CommunityAreaOrZip               string  `json:"community_area_or_zip"`
	CommunityAreaName                string  `json:"community_area_name,omitempty"`
	CCVIScore                        float64 `json:"ccvi_score,string"`
	CCVICategory                     string  `json:"ccvi_category"`
	RankSocioeconomicStatus          int     `json:"rank_socioeconomic_status,string,omitempty"`
	RankHouseholdComposition         int     `json:"rank_household_composition,string,omitempty"`
	RankAdultsNoPCP                  int     `json:"rank_adults_no_pcp,string,omitempty"`
	RankCumulativeMobilityRatio      int     `json:"rank_cumulative_mobility_ratio,string,omitempty"`
	RankFrontlineEssentialWorkers    int     `json:"rank_frontline_essential_workers,string,omitempty"`
	RankAge65Plus                    int     `json:"rank_age_65_plus,string,omitempty"`
	RankComorbidConditions           int     `json:"rank_comorbid_conditions,string,omitempty"`
	RankCovid19IncidenceRate         int     `json:"rank_covid_19_incidence_rate,string,omitempty"`
	RankCovid19HospitalAdmissionRate int     `json:"rank_covid_19_hospital_admission_rate,string,omitempty"`
	RankCovid19CrudeMortalityRate    int     `json:"rank_covid_19_crude_mortality_rate,string,omitempty"`
}

// Declare my database connection
var db *sql.DB

// The main package can has the init function.
// The init function will be triggered before the main function

func init() {
	var err error

	fmt.Println("Initializing the DB connection")

	// Establish connection to Postgres Database

	// OPTION 1 - Postgress application running on localhost
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=localhost sslmode=disable port = 5432"

	// OPTION 2
	// Docker container for the Postgres microservice - uncomment when deploy with host.docker.internal
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=host.docker.internal sslmode=disable port = 5433"

	// OPTION 3
	// Docker container for the Postgress microservice - uncomment when deploy with IP address of the container
	// To find your Postgres container IP, use the command with your network name listed in the docker compose file as follows:
	// docker network inspect cbi_backend
	//db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=162.123.0.9 sslmode=disable port = 5433"

	//Option 4
	//Database application running on Google Cloud Platform.
	db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=/cloudsql/chicago-bi-442520:us-central1:mypostgres sslmode=disable port = 5432"

	db, err = sql.Open("postgres", db_connection)
	if err != nil {
		log.Fatal(fmt.Println("Couldn't Open Connection to database"))
		panic(err)
	}

	// Test the database connection
	//err = db.Ping()
	//if err != nil {
	//	fmt.Println("Couldn't Connect to database")
	//	panic(err)
	//}

}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

func main() {

	for {

		// While using Cloud Run for instrumenting/prototyping/debugging use the server
		// to trace the state of you running data collection services
		// Navigate to Cloud Run services and find the URL of your service
		// An example of your services URL: https://go-microservice-23zzuv4hksp-uc.a.run.app
		// Use the browser and navigate to your service URL to to kick-start your service

		log.Print("starting CBI Microservices ...")

		// Pull the data once a day
		// You might need to pull Taxi Trips and COVID data on daily basis
		// but not the unemployment dataset becasue its dataset doesn't change every day
		// This code snippet is only for prototypying and unit-testing

		// build and fine-tune the functions to pull data from the different data sources
		// The following code snippets show you how to pull data from different data sources

		go GetCommunityAreaUnemployment(db)
		go GetBuildingPermits(db)
		go GetTaxiTrips(db)
		go GetCCVIDetails(db)
		go GetCovidDetails(db)

		http.HandleFunc("/", handler)

		// Determine port for HTTP service.
		port := os.Getenv("PORT")
		if port == "" {
			port = "8080"
			log.Printf("defaulting to port %s", port)
		}

		// Start HTTP server.
		log.Printf("listening on port %s", port)
		log.Print("Navigate to Cloud Run services and find the URL of your service")
		log.Print("Use the browser and navigate to your service URL to to check your service has started")

		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Fatal(err)
		}

		time.Sleep(24 * time.Hour)
	}

}

///////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////

func handler(w http.ResponseWriter, r *http.Request) {
	name := os.Getenv("chicago-bi-442520")
	if name == "" {
		name = "CBI-Project"
	}

	fmt.Fprintf(w, "CBI data collection microservices' goroutines have started for %s!\n", name)
}

/////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////

func GetTaxiTrips(db *sql.DB) {

	// Data Collection needed from two data sources:
	// 1. https://data.cityofchicago.org/Transportation/Taxi-Trips/wrvz-psew
	// 2. https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

	fmt.Println("GetTaxiTrips: Collecting Taxi Trips Data")

	// Get your geocoder.ApiKey from here :
	// https://developers.google.com/maps/documentation/geocoding/get-api-key?authuser=2

	geocoder.ApiKey = "AIzaSyD_1f_pj31WrYj4lrn2pFHN1sM8UwZiQ1s"

	drop_table := `drop table if exists taxi_trips`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "taxi_trips" (
						"id"   SERIAL , 
						"trip_id" VARCHAR(255) UNIQUE, 
						"trip_start_timestamp" TIMESTAMP WITH TIME ZONE, 
						"trip_end_timestamp" TIMESTAMP WITH TIME ZONE, 
						"pickup_centroid_latitude" DOUBLE PRECISION, 
						"pickup_centroid_longitude" DOUBLE PRECISION, 
						"dropoff_centroid_latitude" DOUBLE PRECISION, 
						"dropoff_centroid_longitude" DOUBLE PRECISION, 
						"pickup_zip_code" VARCHAR(255), 
						"dropoff_zip_code" VARCHAR(255), 
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Taxi Trips")

	// Get the the Taxi Trips for Taxi medallions list

	var url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=50"

	tr := &http.Transport{
		MaxIdleConns:          10,
		IdleConnTimeout:       1000 * time.Second,
		TLSHandshakeTimeout:   1000 * time.Second,
		ExpectContinueTimeout: 1000 * time.Second,
		DisableCompression:    true,
		Dial: (&net.Dialer{
			Timeout:   1000 * time.Second,
			KeepAlive: 1000 * time.Second,
		}).Dial,
		ResponseHeaderTimeout: 1000 * time.Second,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)

	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Taxi Trips")

	body_1, _ := ioutil.ReadAll(res.Body)
	var taxi_trips_list_1 TaxiTripsJsonRecords
	json.Unmarshal(body_1, &taxi_trips_list_1)

	// Get the Taxi Trip list for rideshare companies like Uber/Lyft list
	// Transportation-Network-Providers-Trips:
	var url_2 = "https://data.cityofchicago.org/resource/m6dm-c72p.json?$limit=50"

	res_2, err := http.Get(url_2)
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Transportation-Network-Providers-Trips")

	body_2, _ := ioutil.ReadAll(res_2.Body)
	var taxi_trips_list_2 TaxiTripsJsonRecords
	json.Unmarshal(body_2, &taxi_trips_list_2)

	s := fmt.Sprintf("\n\n Transportation-Network-Providers-Trips number of SODA records received = %d\n\n", len(taxi_trips_list_2))
	io.WriteString(os.Stdout, s)

	// Add the Taxi medallions list & rideshare companies like Uber/Lyft list

	taxi_trips_list := append(taxi_trips_list_1, taxi_trips_list_2...)

	// Process the list

	for i := 0; i < len(taxi_trips_list); i++ {

		trip_id := taxi_trips_list[i].Trip_id
		if trip_id == "" {
			continue
		}

		// if trip start/end timestamp doesn't have the length of 23 chars in the format "0000-00-00T00:00:00.000"
		// skip this record

		// get Trip_start_timestamp
		trip_start_timestamp := taxi_trips_list[i].Trip_start_timestamp
		if len(trip_start_timestamp) < 23 {
			continue
		}

		// get Trip_end_timestamp
		trip_end_timestamp := taxi_trips_list[i].Trip_end_timestamp
		if len(trip_end_timestamp) < 23 {
			continue
		}

		pickup_centroid_latitude := taxi_trips_list[i].Pickup_centroid_latitude

		if pickup_centroid_latitude == "" {
			continue
		}

		pickup_centroid_longitude := taxi_trips_list[i].Pickup_centroid_longitude

		if pickup_centroid_longitude == "" {
			continue
		}

		dropoff_centroid_latitude := taxi_trips_list[i].Dropoff_centroid_latitude

		if dropoff_centroid_latitude == "" {
			continue
		}

		dropoff_centroid_longitude := taxi_trips_list[i].Dropoff_centroid_longitude

		if dropoff_centroid_longitude == "" {
			continue
		}

		// Using pickup_centroid_latitude and pickup_centroid_longitude in geocoder.GeocodingReverse
		// we could find the pickup zip-code

		pickup_centroid_latitude_float, _ := strconv.ParseFloat(pickup_centroid_latitude, 64)
		pickup_centroid_longitude_float, _ := strconv.ParseFloat(pickup_centroid_longitude, 64)
		pickup_location := geocoder.Location{
			Latitude:  pickup_centroid_latitude_float,
			Longitude: pickup_centroid_longitude_float,
		}

		// Comment the following line while not unit-testing
		fmt.Println(pickup_location)

		pickup_address_list, _ := geocoder.GeocodingReverse(pickup_location)
		pickup_address := pickup_address_list[0]
		pickup_zip_code := pickup_address.PostalCode

		// Using dropoff_centroid_latitude and dropoff_centroid_longitude in geocoder.GeocodingReverse
		// we could find the dropoff zip-code

		dropoff_centroid_latitude_float, _ := strconv.ParseFloat(dropoff_centroid_latitude, 64)
		dropoff_centroid_longitude_float, _ := strconv.ParseFloat(dropoff_centroid_longitude, 64)

		dropoff_location := geocoder.Location{
			Latitude:  dropoff_centroid_latitude_float,
			Longitude: dropoff_centroid_longitude_float,
		}

		dropoff_address_list, _ := geocoder.GeocodingReverse(dropoff_location)
		dropoff_address := dropoff_address_list[0]
		dropoff_zip_code := dropoff_address.PostalCode

		sql := `INSERT INTO taxi_trips ("trip_id", "trip_start_timestamp", "trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude", "pickup_zip_code", 
			"dropoff_zip_code") values($1, $2, $3, $4, $5, $6, $7, $8, $9)`

		_, err = db.Exec(
			sql,
			trip_id,
			trip_start_timestamp,
			trip_end_timestamp,
			pickup_centroid_latitude,
			pickup_centroid_longitude,
			dropoff_centroid_latitude,
			dropoff_centroid_longitude,
			pickup_zip_code,
			dropoff_zip_code)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Completed Inserting Rows into the TaxiTrips Table")

}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////

func GetCommunityAreaUnemployment(db *sql.DB) {
	fmt.Println("GetCommunityAreaUnemployment: Collecting Unemployment Rates Data")

	drop_table := `drop table if exists community_area_unemployment`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "community_area_unemployment" (
						"id"   SERIAL , 
						"community_area" VARCHAR(255) UNIQUE, 
						"community_area_name" VARCHAR(255), 
						"birth_rate" VARCHAR(255), 
						"general_fertility_rate" VARCHAR(255), 
						"low_birth_weight" VARCHAR(255),												
						"prenatal_care_beginning_in_first_trimester" VARCHAR(255) , 
						"preterm_births" VARCHAR(255), 
						"teen_birth_rate" VARCHAR(255), 
						"assault_homicide" VARCHAR(255), 
						"breast_cancer_in_females" VARCHAR(255),												
						"cancer_all_sites" VARCHAR(255) , 
						"colorectal_cancer" VARCHAR(255), 
						"diabetes_related" VARCHAR(255), 
						"firearm_related" VARCHAR(255), 
						"infant_mortality_rate" VARCHAR(255),						
						"lung_cancer" VARCHAR(255) , 
						"prostate_cancer_in_males" VARCHAR(255), 
						"stroke_cerebrovascular_disease" VARCHAR(255), 
						"childhood_blood_lead_level_screening" VARCHAR(255), 
						"childhood_lead_poisoning" VARCHAR(255),						
						"gonorrhea_in_females" VARCHAR(255) , 
						"gonorrhea_in_males" VARCHAR(255), 
						"tuberculosis" VARCHAR(255), 
						"below_poverty_level" VARCHAR(255), 
						"crowded_housing" VARCHAR(255),						
						"dependency" VARCHAR(255) , 
						"no_high_school_diploma" VARCHAR(255), 
						"unemployment" VARCHAR(255), 
						"per_capita_income" VARCHAR(255),
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for community_area_unemployment")

	// There are 77 known community areas in the data set
	// So, set limit to 100.
	var url = "https://data.cityofchicago.org/resource/iqnk-2tcu.json?$limit=50"

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)

	if err != nil {
		panic(err)
	}

	fmt.Println("Community Areas Unemplyment: Received data from SODA REST API for Unemployment")

	body, _ := ioutil.ReadAll(res.Body)
	var unemployment_data_list UnemploymentJsonRecords
	json.Unmarshal(body, &unemployment_data_list)

	s := fmt.Sprintf("\n\n Community Areas number of SODA records received = %d\n\n", len(unemployment_data_list))
	io.WriteString(os.Stdout, s)

	for i := 0; i < len(unemployment_data_list); i++ {

		community_area := unemployment_data_list[i].Community_area
		if community_area == "" {
			continue
		}

		community_area_name := unemployment_data_list[i].Community_area_name
		if community_area_name == "" {
			continue
		}

		birth_rate := unemployment_data_list[i].Birth_rate

		general_fertility_rate := unemployment_data_list[i].General_fertility_rate

		low_birth_weight := unemployment_data_list[i].Low_birth_weight

		prenatal_care_beginning_in_first_trimester := unemployment_data_list[i].Prenatal_care_beginning_in_first_trimester

		preterm_births := unemployment_data_list[i].Preterm_births

		teen_birth_rate := unemployment_data_list[i].Teen_birth_rate

		assault_homicide := unemployment_data_list[i].Assault_homicide

		breast_cancer_in_females := unemployment_data_list[i].Breast_cancer_in_females

		cancer_all_sites := unemployment_data_list[i].Cancer_all_sites

		colorectal_cancer := unemployment_data_list[i].Colorectal_cancer

		diabetes_related := unemployment_data_list[i].Diabetes_related

		firearm_related := unemployment_data_list[i].Firearm_related

		infant_mortality_rate := unemployment_data_list[i].Infant_mortality_rate

		lung_cancer := unemployment_data_list[i].Lung_cancer

		prostate_cancer_in_males := unemployment_data_list[i].Prostate_cancer_in_males

		stroke_cerebrovascular_disease := unemployment_data_list[i].Stroke_cerebrovascular_disease

		childhood_blood_lead_level_screening := unemployment_data_list[i].Childhood_blood_lead_level_screening

		childhood_lead_poisoning := unemployment_data_list[i].Childhood_lead_poisoning

		gonorrhea_in_females := unemployment_data_list[i].Gonorrhea_in_females

		gonorrhea_in_males := unemployment_data_list[i].Gonorrhea_in_males

		tuberculosis := unemployment_data_list[i].Tuberculosis

		below_poverty_level := unemployment_data_list[i].Below_poverty_level

		crowded_housing := unemployment_data_list[i].Crowded_housing

		dependency := unemployment_data_list[i].Dependency

		no_high_school_diploma := unemployment_data_list[i].No_high_school_diploma

		per_capita_income := unemployment_data_list[i].Per_capita_income

		unemployment := unemployment_data_list[i].Unemployment

		sql := `INSERT INTO community_area_unemployment ("community_area" , 
		"community_area_name" , 
		"birth_rate" , 
		"general_fertility_rate" , 
		"low_birth_weight" ,
		"prenatal_care_beginning_in_first_trimester" , 
		"preterm_births" , 
		"teen_birth_rate" , 
		"assault_homicide" , 
		"breast_cancer_in_females" ,
		"cancer_all_sites"  , 
		"colorectal_cancer" , 
		"diabetes_related" , 
		"firearm_related" , 
		"infant_mortality_rate" ,
		"lung_cancer" , 
		"prostate_cancer_in_males" , 
		"stroke_cerebrovascular_disease" , 
		"childhood_blood_lead_level_screening" , 
		"childhood_lead_poisoning" ,		
		"gonorrhea_in_females"  , 
		"gonorrhea_in_males" , 
		"tuberculosis" , 
		"below_poverty_level" , 
		"crowded_housing" ,		
		"dependency"  , 
		"no_high_school_diploma" , 
		"unemployment" , 
		"per_capita_income" )
		values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,$11, $12, $13, $14, $15,$16, $17, $18, $19, $20,$21, $22, $23, $24, $25,$26, $27, $28, $29)`

		_, err = db.Exec(
			sql,
			community_area,
			community_area_name,
			birth_rate,
			general_fertility_rate,
			low_birth_weight,
			prenatal_care_beginning_in_first_trimester,
			preterm_births,
			teen_birth_rate,
			assault_homicide,
			breast_cancer_in_females,
			cancer_all_sites,
			colorectal_cancer,
			diabetes_related,
			firearm_related,
			infant_mortality_rate,
			lung_cancer,
			prostate_cancer_in_males,
			stroke_cerebrovascular_disease,
			childhood_blood_lead_level_screening,
			childhood_lead_poisoning,
			gonorrhea_in_females,
			gonorrhea_in_males,
			tuberculosis,
			below_poverty_level,
			crowded_housing,
			dependency,
			no_high_school_diploma,
			unemployment,
			per_capita_income)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Completed Inserting Rows into the community_area_unemployment Table")

}

////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////////

func GetBuildingPermits(db *sql.DB) {
	fmt.Println("GetBuildingPermits: Collecting Building Permits Data")

	drop_table := `drop table if exists building_permits`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "building_permits" (
						"id"   SERIAL , 
						"permit_id" VARCHAR(255) UNIQUE, 
						"permit_code" VARCHAR(255), 
						"permit_type" VARCHAR(255),  
						"review_type"      VARCHAR(255), 
						"application_start_date"      VARCHAR(255), 
						"issue_date"      VARCHAR(255), 
						"processing_time"      VARCHAR(255), 
						"street_number"      VARCHAR(255), 
						"street_direction"      VARCHAR(255), 
						"street_name"      VARCHAR(255), 
						"work_description"      TEXT, 
						"building_fee_paid"      VARCHAR(255), 
						"zoning_fee_paid"      VARCHAR(255), 
						"other_fee_paid"      VARCHAR(255), 
						"subtotal_paid"      VARCHAR(255), 
						"building_fee_unpaid"      VARCHAR(255), 
						"zoning_fee_unpaid"      VARCHAR(255), 
						"other_fee_unpaid"      VARCHAR(255), 
						"subtotal_unpaid"      VARCHAR(255), 
						"building_fee_waived"      VARCHAR(255), 
						"zoning_fee_waived"      VARCHAR(255), 
						"other_fee_waived"      VARCHAR(255), 
						"subtotal_waived"      VARCHAR(255), 
						"total_fee"      VARCHAR(255), 
						"contact_1_type"      VARCHAR(255), 
						"contact_1_name"      VARCHAR(255), 
						"contact_1_city"      VARCHAR(255), 
						"contact_1_state"      VARCHAR(255), 
						"contact_1_zipcode"      VARCHAR(255), 
						"reported_cost"      VARCHAR(255), 
						"community_area"      VARCHAR(255), 
						"census_tract"      VARCHAR(255), 
						"ward"      VARCHAR(255), 
						"xcoordinate"      DOUBLE PRECISION ,
						"ycoordinate"      DOUBLE PRECISION ,
						PRIMARY KEY ("id") 
					);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	fmt.Println("Created Table for Building Permits")

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	var url = "https://data.cityofchicago.org/resource/building-permits.json?$limit=50"

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Building Permits")

	body, _ := ioutil.ReadAll(res.Body)
	var building_data_list BuildingPermitsJsonRecords
	json.Unmarshal(body, &building_data_list)

	s := fmt.Sprintf("\n\n Building Permits: number of SODA records received = %d\n\n", len(building_data_list))
	io.WriteString(os.Stdout, s)

	for i := 0; i < len(building_data_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// There are different methods to deal with messy/dirty/missing data.
		// We will use the simplest method: drop records that have messy/dirty/missing data
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		permit_id := building_data_list[i].Id
		if permit_id == "" {
			continue
		}

		permit_code := building_data_list[i].Permit_Code
		if permit_code == "" {
			continue
		}

		permit_type := building_data_list[i].Permit_type
		if permit_type == "" {
			continue
		}

		review_type := building_data_list[i].Review_type
		if review_type == "" {
			continue
		}

		application_start_date := building_data_list[i].Application_start_date
		if application_start_date == "" {
			continue
		}
		issue_date := building_data_list[i].Issue_date
		if issue_date == "" {
			continue
		}
		processing_time := building_data_list[i].Processing_time
		if processing_time == "" {
			continue
		}

		street_number := building_data_list[i].Street_number
		if street_number == "" {
			continue
		}
		street_direction := building_data_list[i].Street_direction
		if street_direction == "" {
			continue
		}
		street_name := building_data_list[i].Street_name
		if street_name == "" {
			continue
		}
		work_description := building_data_list[i].Work_description
		if work_description == "" {
			continue
		}
		building_fee_paid := building_data_list[i].Building_fee_paid
		if building_fee_paid == "" {
			continue
		}
		zoning_fee_paid := building_data_list[i].Zoning_fee_paid
		if zoning_fee_paid == "" {
			continue
		}
		other_fee_paid := building_data_list[i].Other_fee_paid
		if other_fee_paid == "" {
			continue
		}
		subtotal_paid := building_data_list[i].Subtotal_paid
		if subtotal_paid == "" {
			continue
		}
		building_fee_unpaid := building_data_list[i].Building_fee_unpaid
		if building_fee_unpaid == "" {
			continue
		}
		zoning_fee_unpaid := building_data_list[i].Zoning_fee_unpaid
		if zoning_fee_unpaid == "" {
			continue
		}
		other_fee_unpaid := building_data_list[i].Other_fee_unpaid
		if other_fee_unpaid == "" {
			continue
		}
		subtotal_unpaid := building_data_list[i].Subtotal_unpaid
		if subtotal_unpaid == "" {
			continue
		}
		building_fee_waived := building_data_list[i].Building_fee_waived
		if building_fee_waived == "" {
			continue
		}
		zoning_fee_waived := building_data_list[i].Zoning_fee_waived
		if zoning_fee_waived == "" {
			continue
		}
		other_fee_waived := building_data_list[i].Other_fee_waived
		if other_fee_waived == "" {
			continue
		}

		subtotal_waived := building_data_list[i].Subtotal_waived
		if subtotal_waived == "" {
			continue
		}
		total_fee := building_data_list[i].Total_fee
		if total_fee == "" {
			continue
		}

		contact_1_type := building_data_list[i].Contact_1_type
		if contact_1_type == "" {
			continue
		}

		contact_1_name := building_data_list[i].Contact_1_name
		if contact_1_name == "" {
			continue
		}

		contact_1_city := building_data_list[i].Contact_1_city
		if contact_1_city == "" {
			continue
		}
		contact_1_state := building_data_list[i].Contact_1_state
		if contact_1_state == "" {
			continue
		}

		contact_1_zipcode := building_data_list[i].Contact_1_zipcode
		if contact_1_zipcode == "" {
			continue
		}

		reported_cost := building_data_list[i].Reported_cost
		if reported_cost == "" {
			continue
		}

		community_area := building_data_list[i].Community_area

		census_tract := building_data_list[i].Census_tract
		if census_tract == "" {
			continue
		}

		ward := building_data_list[i].Ward
		if ward == "" {
			continue
		}

		xcoordinate := building_data_list[i].Xcoordinate
		if xcoordinate == "" {
			continue
		}

		ycoordinate := building_data_list[i].Ycoordinate
		if ycoordinate == "" {
			continue
		}

		sql := `INSERT INTO building_permits ("permit_id", "permit_code", "permit_type","review_type",
		"application_start_date",
		"issue_date",
		"processing_time",
		"street_number",
		"street_direction",
		"street_name",
		"work_description",
		"building_fee_paid",
		"zoning_fee_paid",
		"other_fee_paid",
		"subtotal_paid",
		"building_fee_unpaid",
		"zoning_fee_unpaid",
		"other_fee_unpaid",
		"subtotal_unpaid",
		"building_fee_waived",
		"zoning_fee_waived",
		"other_fee_waived",
		"subtotal_waived",
		"total_fee",
		"contact_1_type",
		"contact_1_name",
		"contact_1_city",
		"contact_1_state",
		"contact_1_zipcode",
		"reported_cost",
		"community_area",
		"census_tract",
		"ward",
		"xcoordinate",
		"ycoordinate" )
		values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,$11, $12, $13, $14, $15,$16, $17, $18, $19, $20,$21, $22, $23, $24, $25,$26, $27, $28, $29,$30,$31, $32, $33, $34, $35)`

		_, err = db.Exec(
			sql,
			permit_id,
			permit_code,
			permit_type,
			review_type,
			application_start_date,
			issue_date,
			processing_time,
			street_number,
			street_direction,
			street_name,
			work_description,
			building_fee_paid,
			zoning_fee_paid,
			other_fee_paid,
			subtotal_paid,
			building_fee_unpaid,
			zoning_fee_unpaid,
			other_fee_unpaid,
			subtotal_unpaid,
			building_fee_waived,
			zoning_fee_waived,
			other_fee_waived,
			subtotal_waived,
			total_fee,
			contact_1_type,
			contact_1_name,
			contact_1_city,
			contact_1_state,
			contact_1_zipcode,
			reported_cost,
			community_area,
			census_tract,
			ward,
			xcoordinate,
			ycoordinate)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Completed Inserting Rows into the Building Permits Table")
}

////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////
//Sample dataset reviewed:
//"zip_code":"60602",
//"week_number":"35",
//"week_start":"2021-08-29T00:00:00.000",
//"week_end":"2021-09-04T00:00:00.000",
//"cases_weekly":"2",
//"cases_cumulative":"123",
//"case_rate_weekly":"160.8",
//"case_rate_cumulative":"9887.5",
//"tests_weekly":"92",
//"tests_cumulative":"3970",
//"test_rate_weekly":"7395.5",
//"test_rate_cumulative":"319131.8",
//"percent_tested_positive_weekly":"0.022",
//"percent_tested_positive_cumulative":"0.035",
//"deaths_weekly":"0",
//"deaths_cumulative":"2",
//"death_rate_weekly":"0",
//"death_rate_cumulative":"160.8",
//"population":"1244",
//"row_id":"60602-2021-35",
//"zip_code_location":{"type":"Point",
//						"coordinates":
//							0 -87.628309
//							1  41.883136
//":@computed_region_rpca_8um6":"41",
//":@computed_region_vrxf_vc4k":"38",
//":@computed_region_6mkv_f3dw":"14310",
//":@computed_region_bdys_3d7i":"92",
//":@computed_region_43wa_7qmu":"36"
////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////////

func GetCovidDetails(db *sql.DB) {

	drop_table := `drop table if exists covid_data_zipcode`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	createTable := `CREATE TABLE covid_data_zipcode (
		zip_code VARCHAR(255),
		week_number VARCHAR(255),
		week_start VARCHAR(255),
		week_end VARCHAR(255),
		cases_weekly VARCHAR(255),
		cases_cumulative VARCHAR(255),
		case_rate_weekly VARCHAR(255),
		case_rate_cumulative VARCHAR(255),
		tests_weekly VARCHAR(255),
		tests_cumulative VARCHAR(255),
		test_rate_weekly VARCHAR(255),
		test_rate_cumulative VARCHAR(255),
		percent_tested_positive_weekly VARCHAR(255),
		percent_tested_positive_cumulative VARCHAR(255),
		deaths_weekly VARCHAR(255),
		deaths_cumulative VARCHAR(255),
		death_rate_weekly VARCHAR(255),
		death_rate_cumulative VARCHAR(255)
	);`
	_, err = db.Exec(createTable)
	if err != nil {
		fmt.Printf("Failed to create table: %v\n", err)
		return
	}
	fmt.Println("Table created successfully.")

	fmt.Println("Created Table for Zipcode Covid Data")

	// Set the URL for retrieving CCVI Data
	var url = "https://data.cityofchicago.org/resource/yhhz-zm2v.json?$limit=50"

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Zipcode Covid Data")

	// Read and parse the JSON response
	body, _ := ioutil.ReadAll(res.Body)
	var zipcodecovidlist []ZipcodeCovidRecords
	json.Unmarshal(body, &zipcodecovidlist)

	for i := 0; i < len(zipcodecovidlist); i++ {
		// Individual checks for each field
		zipcode := zipcodecovidlist[i].Zip_code
		if zipcode == "" {
			continue
		}

		week_number := zipcodecovidlist[i].Week_number
		if week_number == "" {
			continue
		}

		week_start := zipcodecovidlist[i].Week_start
		if week_start == "" {
			continue
		}

		week_end := zipcodecovidlist[i].Week_end
		if week_end == "" {
			continue
		}

		cases_weekly := zipcodecovidlist[i].Cases_weekly
		if cases_weekly == "" {
			continue
		}

		cases_cumulative := zipcodecovidlist[i].Cases_cumulative
		if cases_cumulative == "" {
			continue
		}

		case_rate_weekly := zipcodecovidlist[i].Case_rate_weekly
		if case_rate_weekly == "" {
			continue
		}

		case_rate_cumulative := zipcodecovidlist[i].Case_rate_cumulative
		if case_rate_cumulative == "" {
			continue
		}

		tests_weekly := zipcodecovidlist[i].Tests_weekly
		if tests_weekly == "" {
			continue
		}

		tests_cumulative := zipcodecovidlist[i].Tests_cumulative
		if tests_cumulative == "" {
			continue
		}

		test_rate_weekly := zipcodecovidlist[i].Test_rate_weekly
		if test_rate_weekly == "" {
			continue
		}

		test_rate_cumulative := zipcodecovidlist[i].Tests_cumulative
		if test_rate_cumulative == "" {
			continue
		}

		percent_tested_positive_weekly := zipcodecovidlist[i].Percent_tested_positive_weekly
		if percent_tested_positive_weekly == "" {
			continue
		}

		percent_tested_positive_cumulative := zipcodecovidlist[i].Percent_tested_positive_cumulative
		if percent_tested_positive_cumulative == "" {
			continue
		}

		deaths_weekly := zipcodecovidlist[i].Deaths_weekly
		if deaths_weekly == "" {
			continue
		}

		deaths_cumulative := zipcodecovidlist[i].Deaths_cumulative
		if deaths_cumulative == "" {
			continue
		}

		death_rate_weekly := zipcodecovidlist[i].Death_rate_weekly
		if death_rate_weekly == "" {
			continue
		}

		death_rate_cumulative := zipcodecovidlist[i].Death_rate_cumulative
		if death_rate_cumulative == "" {
			continue
		}

		//Prepare SQL statement
		sql := `INSERT INTO covid_data_zipcode ("zip_code" ,
			"week_number" ,
			"week_start" ,
			"week_end",
			"cases_weekly",
			"cases_cumulative",
			"case_rate_weekly",
			"case_rate_cumulative",
			"tests_weekly",
			"tests_cumulative",
			"test_rate_weekly",
			"test_rate_cumulative",
			"percent_tested_positive_weekly",
			"percent_tested_positive_cumulative",
			"deaths_weekly",
			"deaths_cumulative",
			"death_rate_weekly",
			"death_rate_cumulative" )
		values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)`

		// Execute SQL statement with record values
		_, err = db.Exec(
			sql,
			zipcode,
			week_number,
			week_start,
			week_end,
			cases_weekly,
			cases_cumulative,
			case_rate_weekly,
			case_rate_cumulative,
			tests_weekly,
			tests_cumulative,
			test_rate_weekly,
			test_rate_cumulative,
			percent_tested_positive_weekly,
			percent_tested_positive_cumulative,
			deaths_weekly,
			deaths_cumulative,
			death_rate_weekly,
			death_rate_cumulative)

		if err != nil {
			panic(err)
		}
	}

}

// //////////////////////////////////////////////////////////////////////////////////
// //////////////////////////////////////////////////////////////////////////////////
// Sample dataset reviewed:
// "geography_type":"CA",
// "community_area_or_zip":"70",
// "community_area_name":"Ashburn",
// "ccvi_score":"45.1",
// "ccvi_category":"MEDIUM",
// "rank_socioeconomic_status":"34",
// "rank_household_composition":"32",
// "rank_adults_no_pcp":"28",
// "rank_cumulative_mobility_ratio":"45",
// "rank_frontline_essential_workers":"48",
// "rank_age_65_plus":"29",
// "rank_comorbid_conditions":"33",
// "rank_covid_19_incidence_rate":"59",
// "rank_covid_19_hospital_admission_rate":"66",
// "rank_covid_19_crude_mortality_rate":"39",
// "location":{"type":"Point",
//
//	"coordinates":
//			0	-87.7083657043
//			1	41.7457577128
//
// ":@computed_region_rpca_8um6":"8",
// ":@computed_region_vrxf_vc4k":"69",
// ":@computed_region_6mkv_f3dw":"4300",
// ":@computed_region_bdys_3d7i":"199",
// ":@computed_region_43wa_7qmu":"30"
// //////////////////////////////////////////////////////////////////////////////////
// //////////////////////////////////////////////////////////////////////////////////
func GetCCVIDetails(db *sql.DB) {

	drop_table := `drop table if exists ccvi`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	createTable := `CREATE TABLE ccvi (
		geography_type TEXT,
		community_area_or_zip Integer,
		ccvi_score REAL,
		ccvi_category TEXT
	);`
	_, err = db.Exec(createTable)
	if err != nil {
		fmt.Printf("Failed to create table: %v\n", err)
		return
	}
	fmt.Println("Table created successfully.")

	fmt.Println("Created Table for Building Permits")

	// Set the URL for retrieving CCVI Data
	var url = "https://data.cityofchicago.org/resource/xhc6-88s9.json?$limit=50"

	tr := &http.Transport{
		MaxIdleConns:       10,
		IdleConnTimeout:    300 * time.Second,
		DisableCompression: true,
	}

	client := &http.Client{Transport: tr}

	res, err := client.Get(url)
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for CCVI Data")

	// Read and parse the JSON response
	body, _ := ioutil.ReadAll(res.Body)
	var communityDataList []CCVIRecords
	json.Unmarshal(body, &communityDataList)

	for i := 0; i < len(communityDataList); i++ {
		// Individual checks for each field
		geography_type := communityDataList[i].GeographyType
		if geography_type == "" {
			continue
		}

		community_area_or_zip := communityDataList[i].CommunityAreaOrZip
		if community_area_or_zip == "" {
			continue
		}

		ccvi_score := communityDataList[i].CCVIScore
		if ccvi_score == 0 {
			continue
		}

		ccvi_category := communityDataList[i].CCVICategory
		if ccvi_category == "" {
			continue
		}

		//Prepare SQL statement
		sql := `INSERT INTO ccvi ("geography_type" ,
			"community_area_or_zip" ,
			"ccvi_score" ,
			"ccvi_category" )
		values ($1, $2, $3, $4)`

		// Execute SQL statement with record values
		_, err = db.Exec(
			sql,
			geography_type,
			community_area_or_zip,
			ccvi_score,
			ccvi_category)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Inserted CCVI into Chicago Business Intelligence Database")

}
