README
Project Overview
This project consists of the implementation and deployment of two primary microservices:

pgAdmin Service: A web-based administration tool for PostgreSQL.
Go Microservice: A custom Go-based microservice designed as part of the project deliverables.

List of Implemented Programs/Microservices
1. pgAdmin Service
Purpose: Provides a GUI for managing PostgreSQL databases.
Docker Image: dpage/pgadmin4 (tagged and hosted on Google Container Registry).

2. Go Microservice
Purpose: Implements connection to Chicago Data Portal and extracts data via SODA API.
Docker Image: Custom-built using the provided Dockerfile and hosted on Google Container Registry.
Taxi Trips: Pulls data from taxi trips and transportation network providers, combines into a single list, and extracts the Zip Code of pickup and dropoff using Google Geocoder.
Building Permits: Pulls data for building permits around Chicago
CommunityAreaUnemployment: Pulls data for socioeconomic by zip code in Chicago
CCVIDetails: Pulls CCVI scores for each Chicago community area/zip code
CovidDetails: Pulls weekly Covid data by zip code for Chicago

Installation and Execution Instructions

Prerequisites
1. Google Cloud SDK installed.
2. Docker installed and configured.
3. Permissions to deploy services to Google Cloud Run.
4. Google Geocoder API key

Installation and Deployment Steps
Step 1: Clone the Repository
git clone https://github.com/ryan-osleeb/chicago_business_intelligence.git
cd chicago_business_intelligence

Step 2: Build and Push Docker Images
The following commands are used in the Cloud Build pipeline defined in cloudbuild.yaml:
pgAdmin Image:
bash

docker pull dpage/pgadmin4
docker tag dpage/pgadmin4 gcr.io/<project-id>/pgadmin
docker push gcr.io/<project-id>/pgadmin
Go Microservice Image:

bash

docker build -t gcr.io/<project-id>/go-microservice .
docker push gcr.io/<project-id>/go-microservice
Step 3: Deploy Services to Cloud Run
Use the Cloud Build pipeline or the commands below:

pgAdmin:

bash
gcloud run deploy pg-admin \
    --image gcr.io/<project-id>/pgadmin \
    --region us-central1 \
    --platform managed \
    --port 80 \
    --allow-unauthenticated \
    --set-env-vars PGADMIN_DEFAULT_EMAIL=user@gmail.com \
    --set-env-vars PGADMIN_DEFAULT_PASSWORD=root

Go Microservice:

bash
gcloud run deploy go-microservice \
    --image gcr.io/<project-id>/go-microservice \
    --region us-central1 \
    --platform managed \
    --port 8080 \
    --allow-unauthenticated

