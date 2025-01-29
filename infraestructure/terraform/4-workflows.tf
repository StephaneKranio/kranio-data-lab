
resource "google_workflows_workflow" "ingestion" {
  name          = "workflow-kranio-data-lab-ingestion-manual-files"
  region        = "us-central1"
  description   = ""
  #service_account = google_service_account.test_account.account_id
  service_account = "projects/kranio-data-lab/serviceAccounts/sr-906@kranio-data-lab.iam.gserviceaccount.com"
  #call_log_level = "LOG_ERRORS_ONLY"
  labels = {
    type = "ingestion"
  }
  source_contents = file("../../data-services/05-workflows/workflow-01-ingestion-manual-files.yml")
}


resource "google_workflows_workflow" "processing" {
  name          = "workflow-kranio-data-lab-processing-manual-files"
  region        = "us-central1"
  description   = ""
  #service_account = google_service_account.test_account.account_id
  service_account = "projects/kranio-data-lab/serviceAccounts/sr-906@kranio-data-lab.iam.gserviceaccount.com"
  #call_log_level = "LOG_ERRORS_ONLY"
  labels = {
    type = "processing"
  }
  source_contents = file("../../data-services/05-workflows/workflow-02-processing-manual-files.yml")
}


resource "google_workflows_workflow" "consumption" {
  name          = "workflow-kranio-data-lab-consumption-manual-files"
  region        = "us-central1"
  description   = ""
  #service_account = google_service_account.test_account.account_id
  service_account = "projects/kranio-data-lab/serviceAccounts/sr-906@kranio-data-lab.iam.gserviceaccount.com"
  #call_log_level = "LOG_ERRORS_ONLY"
  labels = {
    type = "consumption"
  }
  source_contents = file("../../data-services/05-workflows/workflow-03-consumption-manual-files.yml")
}