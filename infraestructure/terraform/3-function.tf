################################## kranio-data-lab-check-dataproc-status: ###############################################
resource "google_storage_bucket_object" "check_dataproc_status" {
    name         = "kranio-data-lab-check-dataproc-status"
    bucket       = google_storage_bucket.function_bucket.name
    content_type = "application/zip"
    source       = "terraform/cloud-functions-files/check_dataproc-status.zip"
}

resource "google_cloudfunctions2_function" "check_dataproc_status_function2" {
    provider    =  google
    name        = "kranio-data-lab-check-dataproc-status"
    location    = "us-central1"

    build_config {
        runtime               = "python312"
        entry_point           = "main"
        source {
          storage_source {        
            bucket = google_storage_bucket.function_bucket.name
            object = google_storage_bucket_object.check_dataproc_status.name
          }
        }
      
    }

    service_config {
        max_instance_count  = 1
        available_memory    = "512M"
        timeout_seconds     = 900
        environment_variables = {
            "PROJECT_ID" = "${var.project_id}"
            "REGION" = "${var.region}"
          }
    }

}


######################################  kranio-data-lab-get-parameters: #######################################
resource "google_storage_bucket_object" "get_parameters_function" {
    name         = "kranio-data-lab-get-parameters"
    bucket       = google_storage_bucket.function_bucket.name
    content_type = "application/zip"
    source       = "terraform/cloud-functions-files/get_parameters.zip"
}

resource "google_cloudfunctions2_function" "get_parameters_function2" {
    provider    =  google
    name        = "kranio-data-lab-get-parameters"
    location    = "us-central1"

    build_config {
        runtime               = "python312"
        entry_point           = "main"

        source {
          storage_source {        
            bucket = google_storage_bucket.function_bucket.name
            object = google_storage_bucket_object.get_parameters_function.name
          }
        }
      
    }

    service_config {
        max_instance_count  = 1
        available_memory    = "512M"
        timeout_seconds     = 900
    }
} 