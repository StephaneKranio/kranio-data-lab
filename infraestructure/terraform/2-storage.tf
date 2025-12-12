
############################## Buckets Manual files #####################################

resource "google_storage_bucket" "kranio_data_lab_ingestion" {
    name     = "kranio-data-lab-ingestion"
    location = var.region
}


resource "google_storage_bucket_object" "ventes" {
  name   = "landing-zone/ventes.csv"
  bucket = google_storage_bucket.kranio_data_lab_ingestion.name
  source = "data/ventes_13_10.csv"
}
resource "google_storage_bucket_object" "clients" {
  name   = "landing-zone/clients.csv"
  bucket = google_storage_bucket.kranio_data_lab_ingestion.name
  source = "data/clients.csv"
}
resource "google_storage_bucket_object" "stocks" {
  name   = "landing-zone/stocks.csv"
  bucket = google_storage_bucket.kranio_data_lab_ingestion.name
  source = "data/stocks.csv"
}
resource "google_storage_bucket_object" "produits" {
  name   = "landing-zone/produits.csv"
  bucket = google_storage_bucket.kranio_data_lab_ingestion.name
  source = "data/produits.csv"
}

resource "google_storage_bucket" "kranio_data_lab_processing" {
    name     = "kranio-data-lab-processing"
    location = var.region
}

resource "google_storage_bucket" "kranio_data_lab_consumption" {
    name     = "kranio-data-lab-consumption"
    location = var.region
}

############################## Buckets scripts and configs ETL #####################################

resource "google_storage_bucket" "kranio_data_lab_scripts_and_configs" {
    name     = "kranio-data-lab-scripts-and-configs"
    location = var.region
}

############################# Download scripts ingestion - processing - consumption ################

resource "google_storage_bucket_object" "scripts_ingestion" {
  name   = "scripts/ingestion_landing_to_raw.py"
  bucket = google_storage_bucket.kranio_data_lab_scripts_and_configs.name
  source = "../../data-services/01-kranio-data-lab-ingestion/ingestion_landing_to_raw.py"
}



resource "google_storage_bucket_object" "scripts_processing" {
  name   = "scripts/processing.py"
  bucket = google_storage_bucket.kranio_data_lab_scripts_and_configs.name
  source = "../../data-services/02-kranio-data-lab-processing/processing.py"
}

resource "google_storage_bucket_object" "scripts_consumption" {
  name   = "scripts/consumption.py"
  bucket = google_storage_bucket.kranio_data_lab_scripts_and_configs.name
  source = "../../data-services/03-kranio-data-lab-consumption/consumption.py"
}


############################## Buckets cloud function #####################################


resource "google_storage_bucket" "function_bucket" {
    name     = "kranio-data-lab-cloud-function"
    location = var.region
}



############################## Buckets terraform #####################################

resource "google_storage_bucket" "terraform_state" {
    name     = "kranio-data-lab-terraform-state"
    location = var.region
}