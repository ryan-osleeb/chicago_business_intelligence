package main

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"database/sql"
	"encoding/json"

	"strconv"

	"github.com/kelvins/geocoder"
	_ "github.com/lib/pq"
)

// Connection details
var (
	Hostname = ""
	Port     = 2345
	Username = ""
	Password = ""
	Database = ""
)

type TaxiTripsRecords []struct {
	Trip_id                    string `json:"trip_id"`
	Trip_start_timestamp       string `json:"trip_start_timestamp"`
	Trip_end_timestamp         string `json:"trip_end_timestamp"`
	Pickup_centroid_latitude   string `json:"pickup_centroid_latitude"`
	Pickup_centroid_longitude  string `json:"pickup_centroid_longitude"`
	Dropoff_centroid_latitude  string `json:"dropoff_centroid_latitude"`
	Dropoff_centroid_longitude string `json:"dropoff_centroid_longitude"`
}

type TNPRecords []struct {
	Trip_id                    string `json:"trip_id"`
	Trip_start_timestamp       string `json:"trip_start_timestamp"`
	Trip_end_timestamp         string `json:"trip_end_timestamp"`
	Pickup_centroid_latitude   string `json:"pickup_centroid_latitude"`
	Pickup_centroid_longitude  string `json:"pickup_centroid_longitude"`
	Dropoff_centroid_latitude  string `json:"dropoff_centroid_latitude"`
	Dropoff_centroid_longitude string `json:"dropoff_centroid_longitude"`
}

type UnemploymentRecords []struct {
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

type BuildingPermitsRecords struct {
	Id             string `json:"id"`
	Permit_Code    string `json:"permit_"`
	Permit_type    string `json:"permit_type"`
	Issue_date     string `json:"issue_date"`
	Total_fee      string `json:"total_fee"`
	Community_area string `json:"community_area"`
	Xcoordinate    string `json:"xcoordinate"`
	Ycoordinate    string `json:"ycoordinate"`
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

type DailyCovidRecords struct {
	Lab_report_date        string `json:"lab_report_date"`
	Cases_total            string `json:"cases_total"`
	Deaths_total           string `json:"deaths_total"`
	Hospitalizations_total string `json:"hospitalizations_total"`
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

type Location struct {
	Type        string     `json:"type"`
	Coordinates [2]float64 `json:"coordinates"`
}

func openConnection() (*sql.DB, error) {
	// Construct connection string
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		Hostname, Port, Username, Password, Database)

	// Open database
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}

	// Check if connection is alive
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	fmt.Println("Successfully connected to the database")
	return db, nil
}

func printTables(db *sql.DB) error {
	// Query to list tables in the public schema
	rows, err := db.Query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
	if err != nil {
		return fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	fmt.Println("Tables in the database:")
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return fmt.Errorf("failed to scan table name: %w", err)
		}
		fmt.Println(tableName)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating over table names: %w", err)
	}

	return nil
}

func BatchTaxiTrips(db *sql.DB) {
	fmt.Println("GetTaxiTrips: Collecting Taxi Trips Data")

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

	var url = "https://data.cityofchicago.org/resource/wrvz-psew.json?$limit=50"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Taxi Trips")

	body, _ := ioutil.ReadAll(res.Body)
	var taxi_trips_list TaxiTripsRecords
	json.Unmarshal(body, &taxi_trips_list)

	for i := 0; i < len(taxi_trips_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

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
		//pickup_centroid_longitude := taxi_trips_list[i].PICKUP_LONG

		if pickup_centroid_longitude == "" {
			continue
		}

		dropoff_centroid_latitude := taxi_trips_list[i].Dropoff_centroid_latitude
		//dropoff_centroid_latitude := taxi_trips_list[i].DROPOFF_LAT

		if dropoff_centroid_latitude == "" {
			continue
		}

		dropoff_centroid_longitude := taxi_trips_list[i].Dropoff_centroid_longitude
		//dropoff_centroid_longitude := taxi_trips_list[i].DROPOFF_LONG

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
		//fmt.Println(pickup_location)

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

		//sql := `INSERT INTO taxi_trips ("trip_id", "trip_start_timestamp", "trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude") values($1, $2, $3, $4, $5, $6, $7)`

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

	fmt.Println("Inserted Taxi Trips into Chicago Business Intelligence Database")
}

func BatchTNPTrips(db *sql.DB) {
	fmt.Println("GetTransportationNetworkProviderTrips: Collecting Transportation Network Provider Data")

	geocoder.ApiKey = "AIzaSyD_1f_pj31WrYj4lrn2pFHN1sM8UwZiQ1s"

	drop_table := `drop table if exists transportation_network_providers`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "transportation_network_providers" (
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

	var url = "https://data.cityofchicago.org/resource/m6dm-c72p.json?$limit=100"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Transportation Network Providers")

	body, _ := ioutil.ReadAll(res.Body)
	var tnp_list TNPRecords
	json.Unmarshal(body, &tnp_list)

	for i := 0; i < len(tnp_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		trip_id := tnp_list[i].Trip_id
		if trip_id == "" {
			continue
		}

		// if trip start/end timestamp doesn't have the length of 23 chars in the format "0000-00-00T00:00:00.000"
		// skip this record

		// get Trip_start_timestamp
		trip_start_timestamp := tnp_list[i].Trip_start_timestamp
		if len(trip_start_timestamp) < 23 {
			continue
		}

		// get Trip_end_timestamp
		trip_end_timestamp := tnp_list[i].Trip_end_timestamp
		if len(trip_end_timestamp) < 23 {
			continue
		}

		pickup_centroid_latitude := tnp_list[i].Pickup_centroid_latitude

		if pickup_centroid_latitude == "" {
			continue
		}

		pickup_centroid_longitude := tnp_list[i].Pickup_centroid_longitude
		//pickup_centroid_longitude := tnp_list[i].PICKUP_LONG

		if pickup_centroid_longitude == "" {
			continue
		}

		dropoff_centroid_latitude := tnp_list[i].Dropoff_centroid_latitude
		//dropoff_centroid_latitude := tnp_list[i].DROPOFF_LAT

		if dropoff_centroid_latitude == "" {
			continue
		}

		dropoff_centroid_longitude := tnp_list[i].Dropoff_centroid_longitude
		//dropoff_centroid_longitude := tnp_list[i].DROPOFF_LONG

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
		//fmt.Println(pickup_location)

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

		sql := `INSERT INTO transportation_network_providers ("trip_id", "trip_start_timestamp", "trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude", "pickup_zip_code",
			"dropoff_zip_code") values($1, $2, $3, $4, $5, $6, $7, $8, $9)`

		//sql := `INSERT INTO transportation_network_providers ("trip_id", "trip_start_timestamp", "trip_end_timestamp", "pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude") values($1, $2, $3, $4, $5, $6, $7)`

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

	fmt.Println("Inserted Transportation Network Trips into Chicago Business Intelligence Database")
}

func BatchUnemploymentRates(db *sql.DB) {
	fmt.Println("GetUnemploymentRates: Collecting Unemployment Rates Data")

	drop_table := `drop table if exists unemployment`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	create_table := `CREATE TABLE IF NOT EXISTS "unemployment" (
		"id" SERIAL PRIMARY KEY, 
		"community_area" VARCHAR(255) UNIQUE, 
		"community_area_name" VARCHAR(255), 
		"birth_rate" VARCHAR(255), 
		"general_fertility_rate" VARCHAR(255), 
		"low_birth_weight" VARCHAR(255), 
		"prenatal_care_beginning_in_first_trimester" VARCHAR(255), 
		"preterm_births" VARCHAR(255), 
		"teen_birth_rate" VARCHAR(255), 
		"assault_homicide" VARCHAR(255), 
		"breast_cancer_in_females" VARCHAR(255), 
		"cancer_all_sites" VARCHAR(255), 
		"colorectal_cancer" VARCHAR(255), 
		"diabetes_related" VARCHAR(255), 
		"firearm_related" VARCHAR(255), 
		"infant_mortality_rate" VARCHAR(255), 
		"lung_cancer" VARCHAR(255), 
		"prostate_cancer_in_males" VARCHAR(255), 
		"stroke_cerebrovascular_disease" VARCHAR(255), 
		"childhood_blood_lead_level_screening" VARCHAR(255), 
		"childhood_lead_poisoning" VARCHAR(255), 
		"gonorrhea_in_females" VARCHAR(255), 
		"gonorrhea_in_males" VARCHAR(255), 
		"tuberculosis" VARCHAR(255), 
		"below_poverty_level" VARCHAR(255), 
		"crowded_housing" VARCHAR(255), 
		"dependency" VARCHAR(255), 
		"no_high_school_diploma" VARCHAR(255), 
		"unemployment" VARCHAR(255), 
		"per_capita_income" VARCHAR(255)
	);`

	_, _err := db.Exec(create_table)
	if _err != nil {
		panic(_err)
	}

	var url = "https://data.cityofchicago.org/resource/iqnk-2tcu.json?$limit=100"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Unemployment")

	body, _ := ioutil.ReadAll(res.Body)
	var unemployment_data_list UnemploymentRecords
	json.Unmarshal(body, &unemployment_data_list)

	for i := 0; i < len(unemployment_data_list); i++ {

		// We will execute defensive coding to check for messy/dirty/missing data values
		// Any record that has messy/dirty/missing data we don't enter it in the data lake/table

		community_area := unemployment_data_list[i].Community_area
		if community_area == "" {
			continue
		}

		community_area_name := unemployment_data_list[i].Community_area_name
		if community_area_name == "" {
			continue
		}

		birth_rate := unemployment_data_list[i].Birth_rate
		if birth_rate == "" {
			continue
		}

		general_fertility_rate := unemployment_data_list[i].General_fertility_rate
		if general_fertility_rate == "" {
			continue
		}

		low_birth_weight := unemployment_data_list[i].Low_birth_weight
		if low_birth_weight == "" {
			continue
		}

		prenatal_care_beginning_in_first_trimester := unemployment_data_list[i].Prenatal_care_beginning_in_first_trimester
		if prenatal_care_beginning_in_first_trimester == "" {
			continue
		}

		preterm_births := unemployment_data_list[i].Preterm_births
		if preterm_births == "" {
			continue
		}

		teen_birth_rate := unemployment_data_list[i].Teen_birth_rate
		if teen_birth_rate == "" {
			continue
		}

		assault_homicide := unemployment_data_list[i].Assault_homicide
		if assault_homicide == "" {
			continue
		}

		breast_cancer_in_females := unemployment_data_list[i].Breast_cancer_in_females
		if breast_cancer_in_females == "" {
			continue
		}

		cancer_all_sites := unemployment_data_list[i].Cancer_all_sites
		if cancer_all_sites == "" {
			continue
		}

		colorectal_cancer := unemployment_data_list[i].Colorectal_cancer
		if colorectal_cancer == "" {
			continue
		}

		diabetes_related := unemployment_data_list[i].Diabetes_related
		if diabetes_related == "" {
			continue
		}

		firearm_related := unemployment_data_list[i].Firearm_related
		if firearm_related == "" {
			continue
		}

		infant_mortality_rate := unemployment_data_list[i].Infant_mortality_rate
		if infant_mortality_rate == "" {
			continue
		}

		lung_cancer := unemployment_data_list[i].Lung_cancer
		if lung_cancer == "" {
			continue
		}

		prostate_cancer_in_males := unemployment_data_list[i].Prostate_cancer_in_males
		if prostate_cancer_in_males == "" {
			continue
		}

		stroke_cerebrovascular_disease := unemployment_data_list[i].Stroke_cerebrovascular_disease
		if stroke_cerebrovascular_disease == "" {
			continue
		}

		childhood_blood_lead_level_screening := unemployment_data_list[i].Childhood_blood_lead_level_screening
		if childhood_blood_lead_level_screening == "" {
			continue
		}

		childhood_lead_poisoning := unemployment_data_list[i].Childhood_lead_poisoning
		if childhood_lead_poisoning == "" {
			continue
		}

		gonorrhea_in_females := unemployment_data_list[i].Gonorrhea_in_females
		if gonorrhea_in_females == "" {
			continue
		}

		gonorrhea_in_males := unemployment_data_list[i].Gonorrhea_in_males
		if gonorrhea_in_males == "" {
			continue
		}

		tuberculosis := unemployment_data_list[i].Tuberculosis
		if tuberculosis == "" {
			continue
		}

		below_poverty_level := unemployment_data_list[i].Below_poverty_level
		if below_poverty_level == "" {
			continue
		}

		crowded_housing := unemployment_data_list[i].Crowded_housing
		if crowded_housing == "" {
			continue
		}

		dependency := unemployment_data_list[i].Dependency
		if dependency == "" {
			continue
		}

		no_high_school_diploma := unemployment_data_list[i].No_high_school_diploma
		if no_high_school_diploma == "" {
			continue
		}

		per_capita_income := unemployment_data_list[i].Per_capita_income
		if per_capita_income == "" {
			continue
		}

		unemployment := unemployment_data_list[i].Unemployment
		if unemployment == "" {
			continue
		}

		sql := `INSERT INTO Unemployment ("community_area" , 
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

}

func BatchBuildingPermits(db *sql.DB) {
	fmt.Println("GetBuildingPermits: Collecting Building Permits Data")

	drop_table := `drop table if exists building_permits`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	createTable := `CREATE TABLE building_permits (
		permit_id VARCHAR(255) UNIQUE,
    	permit_code VARCHAR(255),
    	permit_type VARCHAR(255),
		issue_date VARCHAR(255),
		total_fee VARCHAR(255),
		community_area VARCHAR(255),
		xcoordinate DOUBLE PRECISION ,
		ycoordinate DOUBLE PRECISION 
	);`
	_, err = db.Exec(createTable)
	if err != nil {
		fmt.Printf("Failed to create table: %v\n", err)
		return
	}
	fmt.Println("Table created successfully.")

	fmt.Println("Created Table for Building Permits")

	// While doing unit-testing keep the limit value to 500
	// later you could change it to 1000, 2000, 10,000, etc.
	var url = "https://data.cityofchicago.org/resource/building-permits.json?$limit=100"

	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}

	fmt.Println("Received data from SODA REST API for Building Permits")

	body, _ := ioutil.ReadAll(res.Body)
	var building_data_list []BuildingPermitsRecords
	json.Unmarshal(body, &building_data_list)

	for i := 0; i < len(building_data_list); i++ {

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

		issue_date := building_data_list[i].Issue_date
		if issue_date == "" {
			continue
		}

		total_fee := building_data_list[i].Total_fee
		if total_fee == "" {
			continue
		}

		community_area := building_data_list[i].Community_area
		if total_fee == "" {
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

		sql := `INSERT INTO building_permits ("permit_id", "permit_code", "permit_type", "issue_date", "total_fee", "community_area", "xcoordinate", "ycoordinate") values($1, $2, $3, $4, $5, $6, $7, $8)`

		_, err = db.Exec(
			sql,
			permit_id,
			permit_code,
			permit_type,
			issue_date,
			total_fee,
			community_area,
			xcoordinate,
			ycoordinate)

		if err != nil {
			panic(err)
		}

	}

	fmt.Println("Inserted Building Permits into Chicago Business Intelligence Database")
}

func BatchCCVIData(db *sql.DB) {
	fmt.Println("BatchCCVIData: Collecting CCCVI Data")

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
	var url = "https://data.cityofchicago.org/resource/xhc6-88s9.json?$limit=100"

	// Fetch data from the API
	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()

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

func BatchDailyCovid(db *sql.DB) {
	fmt.Println("BatchDailyCovid: Collecting Daily Covid Data")

	drop_table := `drop table if exists covid_data_daily`
	_, err := db.Exec(drop_table)
	if err != nil {
		panic(err)
	}

	createTable := `CREATE TABLE covid_data_daily (
		lab_report_date VARCHAR(255),
		cases_total VARCHAR(255),
		deaths_total VARCHAR(255),
		hospitalizations_total VARCHAR(255)
	);`
	_, err = db.Exec(createTable)
	if err != nil {
		fmt.Printf("Failed to create table: %v\n", err)
		return
	}
	fmt.Println("Table created successfully.")

	fmt.Println("Created Table for Daily Covid Data")

	// Set the URL for retrieving CCVI Data
	var url = "https://data.cityofchicago.org/resource/naz8-j4nc.json?$limit=100"

	// Fetch data from the API
	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()

	fmt.Println("Received data from SODA REST API for Daily Covid Data")

	// Read and parse the JSON response
	body, _ := ioutil.ReadAll(res.Body)
	var dailycovidlist []DailyCovidRecords
	json.Unmarshal(body, &dailycovidlist)

	for i := 0; i < len(dailycovidlist); i++ {
		// Individual checks for each field
		lab_report_date := dailycovidlist[i].Lab_report_date
		if lab_report_date == "" {
			continue
		}

		cases_total := dailycovidlist[i].Cases_total
		if cases_total == "" {
			continue
		}

		deaths_total := dailycovidlist[i].Deaths_total
		if deaths_total == "" {
			continue
		}

		hospitalizations_total := dailycovidlist[i].Hospitalizations_total
		if hospitalizations_total == "" {
			continue
		}

		//Prepare SQL statement
		sql := `INSERT INTO covid_data_daily ("lab_report_date" ,
			"cases_total" ,
			"deaths_total" ,
			"hospitalizations_total" )
		values ($1, $2, $3, $4)`

		// Execute SQL statement with record values
		_, err = db.Exec(
			sql,
			lab_report_date,
			cases_total,
			deaths_total,
			hospitalizations_total)

		if err != nil {
			panic(err)
		}

	}
}

func BatchZipcodeCovid(db *sql.DB) {
	fmt.Println("BatchZipcodeCovid: Collecting Zipcode Covid Data")

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
	var url = "https://data.cityofchicago.org/resource/yhhz-zm2v.json?$limit=100"

	// Fetch data from the API
	res, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()

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

func main() {

	// Hostname = "localhost"
	// Port = 5433
	// Username = "postgres"
	// Password = "root"
	// Database = "chicago_business_intelligence"

	// db, err := openConnection()
	// if err != nil {
	// 	fmt.Println("Error:", err)
	// 	return
	// }
	// defer db.Close()

	var db *sql.DB
	var err error

	db_connection := "user=postgres dbname=chicago_business_intelligence password=root host=/cloudsql/chicago-bi-442520:us-central1:mypostgres sslmode=disable port = 5432"

	db, err = sql.Open("postgres", db_connection)
	if err != nil {
		fmt.Println("Error:", err)
		panic(err)
	}

	// Print tables in the database
	// if err := printTables(db); err != nil {
	// 	fmt.Println("Error:", err)
	// }

	//Run batch functions individually
	BatchTaxiTrips(db)
	BatchTNPTrips(db)
	BatchBuildingPermits(db)
	BatchUnemploymentRates(db)
	BatchCCVIData(db)
	BatchDailyCovid(db)
	BatchZipcodeCovid(db)

	// WaitGroup to manage concurrency
	// var wg sync.WaitGroup

	// List of batch functions
	// batchFunctions := []func(*sql.DB){
	// 	BatchTaxiTrips,
	// 	BatchTNPTrips,
	// 	BatchBuildingPermits,
	// 	BatchUnemploymentRates,
	// 	BatchCCVIData,
	// 	BatchDailyCovid,
	// 	BatchZipcodeCovid,
	// }

	// // Launch each batch function as a Goroutine
	// for _, batchFunction := range batchFunctions {
	// 	wg.Add(1) // Increment the WaitGroup counter
	// 	go func(batchFunc func(*sql.DB)) {
	// 		defer wg.Done() // Decrement the counter when done
	// 		batchFunc(db)
	// 	}(batchFunction)
	// }

	// // Wait for all batch functions to complete
	// wg.Wait()

	// fmt.Println("All batch functions completed.")

	// if _, err := db.Exec("DROP TABLE IF EXISTS building_permits"); err != nil {
	// 	fmt.Printf("Failed to drop table: %v\n", err)
	// 	return
	// }
	// fmt.Println("Table dropped successfully (if it existed).")

	// // Create the ccvi table schema
	// createTable := `CREATE TABLE building_permits (
	// 	permit_id VARCHAR(255) UNIQUE,
	// 	permit_code VARCHAR(255),
	// 	permit_type VARCHAR(255),
	// 	issue_date VARCHAR(255),
	// 	total_fee VARCHAR(255),
	// 	community_area VARCHAR(255),
	// 	xcoordinate DOUBLE PRECISION ,
	// 	ycoordinate DOUBLE PRECISION
	// );`
	// _, err = db.Exec(createTable)
	// if err != nil {
	// 	fmt.Printf("Failed to create table: %v\n", err)
	// 	return
	// }
	// fmt.Println("Table created successfully.")

}
